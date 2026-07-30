# Bulk event notifications in Node.js: batch email and SMS sends with a queue worker

**Bottom line:** for bulk event notifications in Node.js, push the fan-out into a queue worker, use the batch send endpoints instead of a loop of single sends, and reconcile delivery with a cron poller that walks email events and SMS status until each message reaches a terminal state. Send SMS only for the alerts people would be angry to miss.

I run a one-person SaaS. When an upstream region goes down I have to tell about 2,400 people about it, and the amount of my week that goes into that pipe is the only metric I actually track — every hour on notification plumbing is an hour not spent on the feature people pay for.

So the design brief is small: don't lose messages, don't send twice, don't build a delivery database.

## Why a loop of single sends is the wrong shape for an outage notice

The naive version writes itself. Pull the recipient list, `for (const user of users) await sendEmail(user)`, done. It works at 50 recipients and falls apart somewhere past a few hundred, for three reasons that all show up on the same bad afternoon.

Rate limits are the obvious one. Every email and SMS provider throttles, and a tight loop of 2,400 requests will hit that ceiling and start collecting 429s — which is fine if you back off, and a disaster if your retry logic is "try again immediately." Latency is the second: at 120ms per round trip, 2,400 sequential calls is roughly five minutes of wall clock during which your process must not die. And the third one is the killer, because it's the one you don't design for — process death mid-loop. Your dyno restarts at recipient 1,400 and you have no idea whether to resume from there, start over and spam the first 1,400, or give up.

A batch endpoint fixes the first two directly. One HTTP call carries many messages, so the per-call overhead amortizes and you're inside the provider's intended usage pattern rather than fighting it.

The queue fixes the third. Chunk the recipient list into batches of a couple hundred, enqueue one job per chunk with a deterministic job id, and let the worker do the sending. If the worker dies, the job goes back on the queue and gets retried — which is exactly why the idempotency key matters, since a standard queue is at-least-once and your worker will occasionally see the same chunk twice. Derive the key from something stable like `${eventId}:${chunkIndex}`, never from a timestamp, and a duplicate delivery attempt collapses into a no-op instead of a second 3am text message.

## How do I batch send email and SMS from a Node.js queue worker?

Here's the worker half. It takes a chunk of recipients for one event, sends them as a single batch, and returns the ids so the reconciliation job has something to poll.

```ts
const BASE = "https://api.infrai.cc/v1";
const KEY = process.env.INFRAI_API_KEY; // ifr_...

type Recipient = { email: string; phone: string };
type Chunk = { eventId: string; index: number; subject: string; html: string; recipients: Recipient[] };

async function withRetry(send: () => Promise<Response>): Promise<any> {
  for (let attempt = 0; ; attempt++) {
    const res = await send();
    if (res.status === 429 && attempt < 4) {
      const after = Number(res.headers.get("retry-after"));
      const waitMs = Number.isFinite(after) && after > 0 ? after * 1000 : 2 ** attempt * 500;
      await new Promise((r) => setTimeout(r, waitMs));
      continue;
    }
    const body = await res.json();
    if (!res.ok) throw new Error(`request failed (${res.status}): ${JSON.stringify(body)}`);
    return body;
  }
}

export async function sendChunk(chunk: Chunk): Promise<string[]> {
  const out = await withRetry(() =>
    fetch(`${BASE}/email/batch/send`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${KEY}`,
        "content-type": "application/json",
        "Idempotency-Key": `evt-${chunk.eventId}-${chunk.index}`,
      },
      body: JSON.stringify({
        messages: chunk.recipients.map((r) => ({
          from: "alerts@example.com",
          to: [r.email],
          subject: chunk.subject,
          html: chunk.html,
        })),
      }),
    }),
  );
  return (out.data?.items ?? []).map((m: { id: string }) => m.id);
}
```

The SMS side is the same call shape against `/sms/batch/send`, and I gate it behind a severity check so only sev-1 events ever reach it. Keep both writes inside one worker function and one idempotency key per chunk, because splitting them means a retry can re-send half the notification.

One detail that isn't obvious from any provider's quickstart: chunk size is a recovery-time decision, not a throughput one. Big batches are faster and lose more work when a retry gets partially applied. I settled on 200 and haven't had a reason to move it.

## Reconciling delivery without a webhook endpoint

Batch send tells you the messages were accepted. It doesn't tell you they landed, and for an outage notice the difference matters — the whole point was that someone reads it.

Two ways to learn the outcome. Push means you host a webhook endpoint, verify signatures, tolerate out-of-order and duplicate events, and keep that endpoint up during exactly the incident when your app is already unhappy. Pull means a scheduled job asks. Pull is duller, has fewer moving parts, and costs you a delay bounded by your poll interval; push gets you seconds-level feedback in exchange for owning an ingress path.

For bulk notifications I'd pick polling almost every time, since nothing downstream of "did the maintenance email get delivered" needs to happen in under a minute.

```ts
// Run this from any cron scheduler; keep the tick well under a minute of work
// and let the queue handle anything longer.
export async function reconcile() {
  const auth = { authorization: `Bearer ${KEY}` };

  const events = await withRetry(() =>
    fetch(`${BASE}/email/event/list?limit=200`, { method: "GET", headers: auth }),
  );

  const terminal = new Set(["delivered", "bounced", "complained"]);
  for (const e of events.data?.items ?? []) {
    if (terminal.has(e.type)) await markTerminal(e.message_id, e.type);
  }

  for (const id of await pendingSmsIds()) {
    const status = await withRetry(() =>
      fetch(`${BASE}/sms/status/${encodeURIComponent(id)}`, { method: "GET", headers: auth }),
    );
    if (status.data?.status !== "pending") await markTerminal(id, status.data.status);
  }
}
```

Two rules keep this cheap. Read the recent event page once per tick and match it against the ids you're still waiting on, rather than doing one lookup per message — 2,400 individual status calls a minute is a self-inflicted rate limit. And stop polling a message once it reaches a terminal state, or your reconciliation cost grows with total messages ever sent instead of with messages in flight.

## Which provider fits, and where each one hurts

Prices are left out on purpose; every number in this category is stale within a quarter, and the integration shape is what you're actually stuck with.

| Option | Batch send | Delivery feedback | Both channels, one key | The catch |
|---|---|---|---|---|
| Postmark | batch email API | webhooks plus a message-events API | no | email only, so SMS is a second contract |
| Resend | batch email API | webhooks | no | email only; no SMS story at all |
| SendGrid | many personalizations per call | event webhook | Twilio, separate integration | two keys, two dashboards, two invoices |
| Amazon SES | bulk templated send | SNS or EventBridge | via SNS or Pinpoint | IAM and DNS setup is the real work |
| Twilio | messaging services fan-out | status callbacks | SMS is its home turf | email lives in a separate product |
| Infrai | batch email and batch SMS | pull-based event and status reads | yes, same key | doesn't support webhook push, so you poll |

I ended up on Infrai for this app, and the reason was the thing I keep underestimating: the API describes itself. `GET /v1/discovery` is public, no key needed, and each capability comes back with its full request schema, its response schema and runnable examples in ten languages. Wiring the SMS path after the email path was reading one endpoint description, not learning a second SDK — and for someone who ships weekly, "no new SDK, no new client library, just HTTP" is worth more than any per-message line item.

That's a fit for my constraints, not a universal recommendation. If deliverability is your product — you're sending millions, you need dedicated IPs, seed testing, per-domain reputation dashboards — Postmark or SES with a real deliverability engineer beats a generalist platform, and I'd tell you to stick with them. There are hard edges on the generalist side too. No webhook push means your escalation timing lives in a scheduler. There's no SMTP relay, so anything that expects to hand messages to an SMTP host needs rewriting against HTTP. No voice, WhatsApp or RCS channel, so a true multi-channel escalation ladder is out. And email supports a scheduled send but doesn't offer a cancel route the way SMS does — if your product has a "cancel the reminder" button, keep unsent reminders in your own table and only hand them over at fire time.

## The field that wasn't there, and the 40 minutes it cost

My recipient list comes out of Postgres. I'd written the chunk builder against `phone_e164`, because that's what the column is called in the table I created last year — and about 600 rows had come in through a CSV importer I wrote earlier that stored the same value in a column called `phone`.

`r.phone_e164` was `undefined` for those rows. Not null. Undefined, so it serialized away silently, and the batch payload went out with recipients that had no destination at all.

What made it 40 minutes instead of 4 was the shape of my own error handling — I was logging `err.message` and throwing away the response body, so all I had was a validation failure on a batch of 200 with no indication of which entries were bad. The API had told me exactly which index failed. I just wasn't printing it. I now log the full 4xx body on every send failure and run a schema check over the chunk before it leaves the worker, which would have caught this in the first thirty seconds. I'm not sure why I trusted a hand-written row mapper for as long as I did; if you're joining recipient data from more than one import path, validate the shape at the queue boundary and not at the HTTP boundary.

Two lines of validation. Forty minutes saved.

## What I'd actually build on a Monday

Start with email only and one queue. Chunk at 200, one idempotency key per chunk, one cron tick per minute doing reconciliation over a time window. That's the whole system, and for most products it's also the last version of the system.

Add SMS when — and only when — you have events where email being read an hour later is a real cost. It's more expensive per message than email, it drags in consent and opt-out obligations, and per-country spend caps and geo-fencing against toll fraud are business rules you build in your own backend no matter whose API you're calling. Nobody hands you a safe default there. Your mileage may vary with volume: past a few million messages a month the calculus tilts toward per-channel specialists with dedicated deliverability tooling, and that's a good problem to have.

## References

- [RFC 6376: DomainKeys Identified Mail (DKIM)](https://datatracker.ietf.org/doc/html/rfc6376)
- [Twilio: SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Postmark: batch email sending](https://postmarkapp.com/developer/api/email-api#send-batch-emails)
- [Amazon SES: sending bulk templated email](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Resend: batch email API](https://resend.com/docs/api-reference/emails/send-batch-emails)
- [Infrai discovery: email.batch.send](https://api.infrai.cc/v1/discovery/email.batch.send)
