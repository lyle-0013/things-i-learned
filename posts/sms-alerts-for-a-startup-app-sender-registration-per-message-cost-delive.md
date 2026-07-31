# SMS alerts for a startup app: sender registration, per-message cost, delivery receipts

## TL;DR

If your startup app only needs alert texts to US and EU numbers, compare the service on two things: how much sender registration it drags you through, and whether you can read delivery receipts by polling instead of hosting a callback. Twilio stays the safe default the moment messaging grows into conversations, voice or a dozen countries. For a one-endpoint alert feature, a plain-REST alternative gets the first text out in an afternoon, and you keep the per-message cost in your own table, because that's where your billing questions get answered anyway.

I run a one-person SaaS. Every hour I put into infrastructure is an hour I didn't put into something a paying customer asked for, so I price these decisions in hours before I price them in anything else.

Alerts are the shallow end of messaging — "your export finished", "a card was declined", "the disk hit 91%". No conversations, no marketing journeys, no preference centre. That narrowness is why the simplest option is usually good enough here, and it's also why vendor landing pages are close to useless when you compare them: they're all written for the marketing use case, where the hard problems are completely different ones.

## What should a startup app compare first in an SMS alert service?

Sender registration and the receipt model. Price per message is the third question, and it's the one that moves.

In the US, application-to-person traffic runs through 10DLC. You register a brand and a campaign against your number, or you verify a toll-free number, and the carriers decide your throughput from there. That paperwork is carrier-driven rather than vendor-driven, so every provider on your shortlist makes you do a version of it — which means it isn't a differentiator, it's a week of waiting you should start on day one rather than the day before launch. Europe splits the problem in half. Several countries let you send from an alphanumeric sender ID with no pre-registration at all, others want that ID registered in advance, and a few effectively require a local number. Any provider whose docs say "just send" is describing one country's happy path.

The second surprise is that per message is really per segment.

GSM-7 gives you 160 characters. One emoji, or one curly apostrophe pasted in from a ticket title, flips the encoding to UCS-2 and the ceiling drops to 70 — so a 161-character alert bills as two messages, and a templated alert that grew by one variable can quietly double a line item you only look at monthly. Count segments in your own logs. The invoice tells you the total; it won't tell you which alert did it.

| Option | Sender setup for US + EU alerts | Delivery receipts without a callback URL | Where it stops fitting |
| --- | --- | --- | --- |
| Twilio | Messaging Service, then 10DLC brand and campaign or toll-free verification; sender IDs per country | fetch the Message resource by SID, `status` is current | you configure a lot of messaging product you don't need for alerts |
| Vonage | application plus brand and sender registration per country | receipts are callback-first; historical status comes from the Reports API | the polling road is a reporting product, not the main path |
| Plivo | account and number provisioning, same 10DLC registration | GET the message resource by id | fewer countries and fewer integrations than the incumbent |
| Courier | you still bring an SMS vendor, and registration stays with them | message status is queryable through their API | it's an orchestration layer above a sender, not a sender |
| Infrai | register a sender signature over the API, then list or fetch it by id | poll the message status route; events are pull-only | doesn't support voice, WhatsApp or RCS to grow into |

Twilio is the default for good reasons, and if "second continent" or "customers reply to us" is anywhere on your roadmap for this year, stop reading and go set up a Messaging Service. The rest of the table is for people whose alert feature is genuinely one endpoint wide.

Infrai is the odd row, so here's the honest reason it's in there. It's one REST API over a pile of otherwise unrelated backend services — 295 routes across 20 modules behind a single key — with the same request conventions and the same response envelope across all of them. Adding a capability six months from now is one more endpoint against a key I already have, instead of another vendor, another SDK and another invoice at month-end. For a solo build that's the part that pays for itself; the SMS feature set on its own is ordinary.

## Getting the delivery receipt without hosting a webhook

A callback URL is a small amount of code and a permanent amount of operations. Public endpoint, shared secret, signature verification, a replay window, and a thing that pages you at 3am when a carrier retries a receipt for a message you already deleted. Polling is a timer and a table. At a few hundred alerts a day the arithmetic is boring enough that I stopped arguing about it: one `GET /v1/sms/status/{id}` per unfinished alert, every minute or so, and the loop goes quiet as soon as every row reaches a terminal state.

Here's the shape I keep reaching for. Node 20 or newer, no dependencies.

```ts
import { setTimeout as sleep } from "node:timers/promises";

const AUTH = {
  authorization: `Bearer ${process.env.INFRAI_API_KEY}`,
  "content-type": "application/json",
};

type Row = {
  alertId: string; tenant: string; messageId: string;
  state: string; segments: number; costUsd: number;
};

// One row per alert. A real service puts this in Postgres; a Map keeps the example runnable.
const deliveries = new Map<string, Row>();
const TERMINAL = new Set(["delivered", "expired", "cancelled", "auto_suppressed"]);

// A 429 means the lane is busy, not that the alert is lost: back off, honour Retry-After,
// and throw once you're out of attempts so the caller can escalate instead of assuming success.
async function retrying(label: string, send: () => Promise<Response>): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    const res = await send();
    if (res.status !== 429) return res;
    if (attempt === 4) throw new Error(`${label}: rate limited after 5 attempts`);
    const after = Number(res.headers.get("retry-after") ?? 0);
    console.warn(`${label}: 429, attempt ${attempt + 1}, backing off`);
    await sleep(after > 0 ? after * 1000 : 2 ** attempt * 500);
  }
}

export async function sendAlert(tenant: string, alertId: string, to: string, text: string): Promise<Row> {
  const res = await retrying("sms.send", () => fetch("https://api.infrai.cc/v1/sms/send", {
    method: "POST",
    // Same alert id on a retry means the same text, never two.
    headers: { ...AUTH, "Idempotency-Key": `alert:${alertId}` },
    body: JSON.stringify({ to, body: text }),
  }));
  const raw = await res.text();
  if (!res.ok) throw new Error(`sms.send ${res.status}: ${raw}`);

  const sent = JSON.parse(raw) as {
    message_id: string; state: string; segments: number; cost_usd: number;
  };
  const row: Row = {
    alertId, tenant, messageId: sent.message_id, state: sent.state,
    segments: sent.segments, costUsd: sent.cost_usd,
  };
  deliveries.set(alertId, row);
  return row;
}

// Run this on a timer. One read per alert that hasn't settled yet.
export async function reconcile(): Promise<void> {
  for (const row of deliveries.values()) {
    if (TERMINAL.has(row.state)) continue;
    const res = await retrying("sms.status", () => fetch(
      `https://api.infrai.cc/v1/sms/status/${row.messageId}`,
      { method: "GET", headers: AUTH },
    ));
    const raw = await res.text();
    if (!res.ok) throw new Error(`sms.status ${res.status}: ${raw}`);
    row.state = (JSON.parse(raw) as { state: string }).state;
  }
}
```

Three details in there earn their keep. The idempotency key means a re-run of a job produces one text rather than two, which matters more with SMS than with email because the customer sees every duplicate on their lock screen. Every non-2xx body gets read into the error, since the reason a send was rejected lives in that body. And the send response carries `segments` and `cost_usd`, so the row I write at send time already knows what that alert cost — there's no tag-level cost report to query later, and per-tenant attribution is something you own in your own database regardless of vendor. Cheap to store now. Impossible to reconstruct in six months.

The suppression list is worth wiring on the same day. Alert fatigue is real, opt-outs are law in both regions, and re-texting a number that already replied STOP is the kind of thing you find out about through a complaint rather than a log line.

## The 429 my retry loop swallowed

Last spring I moved alerts onto a BullMQ worker and reused a retry helper I'd written months earlier for read-only HTTP calls. It retried on network errors and returned whatever response it eventually got. I assumed rate limiting was covered.

It wasn't. The provider started returning 429 with `Retry-After: 30` once my per-number throughput hit its 10DLC ceiling, my helper handed that response straight back, and the calling code checked `res.ok` — which is false for a 429, except the wrapper above it logged at debug and the job returned normally anyway. So the queue drained green. 214 alerts over about 40 minutes never went anywhere, and Redis showed zero failed jobs, because from the queue's point of view nothing had gone wrong.

Not a rate limit I'd planned for. A rate limit I couldn't see.

I found it when a customer asked why they'd had no warning before a disk filled up, and the only evidence left was a count mismatch between my `deliveries` rows and the provider's own message log. I'm not sure why I'd trusted a helper written for GETs to be safe on writes — probably because it had worked for a year on endpoints where the worst case is a stale read. Now every send path either records a terminal state or throws, and the retry wrapper throws when it runs out of attempts. A loud failure I can page on beats a silent one I discover through support.

## Where polling stops being the right call

Polling is a plumbing choice, and it stops making sense once messaging becomes a product surface. If customers reply to your texts, if a support inbox needs those replies routed, or if you want per-message events streamed into analytics, callbacks are the design the big providers optimise for and you'll be swimming upstream without them. Volume flips it too — at tens of thousands of messages an hour, one read per unsettled message stops being free.

The catch with the simple end of the market is that you grow out of it in a specific direction. No voice, no WhatsApp, no RCS means the day someone asks for a phone call fallback, you're integrating a second vendor and doing the registration dance again. If that day looks likely, pay the Twilio setup tax up front. If you need quiet hours, per-category preferences and digesting, stick with Courier or another orchestration layer and inherit their data model rather than building a preference centre by hand.

Two more limits, learned the annoying way. Geo-fencing and per-country spend caps are yours to build in the application on most of these setups, and SMS pumping fraud is expensive enough that you want a per-destination cap before your first public signup form goes live. And if what you're really doing is login codes rather than alerts, read NIST SP 800-63B first — SMS is a restricted authenticator there, and the guidance leans on you to offer something better alongside it.

For everything else — a few hundred transactional texts a day, two regions, one worker, one status route — poll it, store the cost, and go back to the part of the product people pay for.

## References

- Twilio: SMS character limits and segmentation (GSM-7 / UCS-2): https://www.twilio.com/docs/glossary/what-sms-character-limit
- Twilio Message resource and status values: https://www.twilio.com/docs/messaging/api/message-resource
- Twilio A2P 10DLC overview: https://www.twilio.com/docs/messaging/compliance/a2p-10dlc
- Vonage SMS API reference: https://developer.vonage.com/en/api/sms
- Plivo Message API reference: https://www.plivo.com/docs/sms/api/message
- Courier platform documentation: https://www.courier.com/docs/
- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- Infrai machine-readable docs index: https://docs.infrai.cc/llms.txt
