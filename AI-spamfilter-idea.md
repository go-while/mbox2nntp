# AI spam filtering for imported subjects — design note

Status: idea / not started. Written 2026-10-15 from a side question during the code review.
Nothing here is implemented in `mbox2nntp`.

## The question

Measured on `cll.txt`:

```
grep ^Subject cll.txt | wc -c          12769335
grep ^Subject cll.txt | wc -w           1980104
grep ^Subject cll.txt | sort -u | wc -c  2423506
grep ^Subject cll.txt | sort -u | wc -w   360833
```

| | bytes | words | bytes/word |
|---|---|---|---|
| all Subject lines | 12,769,335 | 1,980,104 | 6.45 |
| unique | 2,423,506 | 360,833 | 6.72 |

Dedup is worth **5.3× by bytes**. The near-identical bytes/word ratio on both sets says the
duplicates are ordinary repeats (`Re:` chains, cross-posts), not a structurally different
subset — so the unique set is representative and classifying it loses nothing.

### Token estimate

Two independent anchors, since neither is reliable alone:

| | bytes ÷ 4 | words × 1.33 | bytes ÷ 3.3 (non-prose) |
|---|---|---|---|
| all | 3.19 M | 2.63 M | 3.87 M |
| unique | 606 k | 480 k | 734 k |

Call it **~3.2 M tokens for the whole file, ~610 k deduped**, ranges 2.6–3.9 M and 0.48–0.73 M.

Expect the upper half of each range. Subject lines tokenize worse than prose: `Re:`/`Fwd:`
chains, `[list-tag]` brackets, version strings, newsgroup names, and especially MIME-encoded
words (`=?iso-8859-1?Q?...?=`), which run roughly 1 token per 2–3 characters. ClariNet-era
traffic has a meaningful fraction of those.

Caveat: `wc -l` was not among the numbers, so the ~260 k / ~48 k line counts below are
inferred from words-per-line and are the softest figures here. The token totals come straight
from bytes and words and do not depend on them.

### Two reductions that cost nothing

1. Strip the literal `Subject: ` prefix — 9 bytes × ~48 k unique lines, ~20% of the unique
   byte count, ~3 tokens per line. It carries zero information once you know what the file is.
2. Decode MIME-encoded words first. Stops the worst tokenizer blowup.

Together those plausibly land the unique set near **450–500 k tokens**.

## Model choice

**ShieldGemma is the wrong tool.** It is a *safety* classifier — harassment, hate speech,
sexually explicit, dangerous content. Spam is not one of its heads. `MAKE MONEY FAST!!! $$$$`
comes back clean, because it is not harmful, it is just spam. Using it here measures the wrong
axis and returns a confident answer about it.

Use a plain instruct model: Gemma 3 4B/12B-IT locally, or Haiku. If a safety pass is also
wanted — ClariNet-era Usenet has its own problems — that is a second, separate label, and
ShieldGemma is the right model for *that* one.

## Budget

Deduped pass, 100 subjects per request:

| | |
|---|---|
| subjects in | ~500 k tokens (prefix stripped) |
| requests | ~480 |
| output (terse labels) | ~100 k tokens |

Under a couple of dollars at Haiku-class pricing with the system prompt cached across all 480
calls. Free locally on Gemma, roughly half an hour on one GPU with vLLM or llama.cpp batching.

**Cost is not the constraint.** Optimise the design for label quality and auditability.

## Batching mechanics

- Emit `index:label`, one per line. Per-item JSON with reasoning multiplies output tokens
  10–20×, and output is the expensive half.
- `sort -u` output is alphabetical, so campaign variants arrive clustered. Cuts both ways:
  good within-campaign consistency, but an all-spam batch invites drift toward labelling
  everything spam. Shuffle for the main pass, then spot-check a sorted run to catch campaigns
  the shuffle split apart.
- Three labels — `spam` / `ham` / `unsure`. The `unsure` pile is where the prompt's gaps show.

## Cascade

1. **Free heuristics first.** Usenet spam subjects are not subtle: all-caps ratio, `$`,
   excessive `!`, `***`, phone numbers, the MMF strings. A few dozen regexes clear a large
   fraction in milliseconds, deterministically and auditably. Diff their verdicts against the
   model's on a sample to find out which one is wrong.
2. **Model on the remainder.**
3. **Then distil.** Label 5–10 k with Haiku, train fastText or logistic regression on
   character 3–5 grams, run that over everything including all future imports at ~1 M lines/s.
   For an ongoing multi-TB ingest this is the shape that survives: the LLM becomes a one-off
   labelling tool, not a dependency in the hot path.

## The signal not being used

Subject text is the **weakest** spam signal available here. The strong one is crosspost count
— the Breidbart Index, which is how Usenet spam was actually defined and cancelled: sum the
square roots of the newsgroup counts across copies; over ~20 is spam essentially by definition.
`mbox2nntp` already parses full articles, so `Newsgroups:` is right there. Also cheap and
strong: identical body across many message-ids, and sender reputation per `From:` domain.

Subjects alone will miss targeted spam and flag legitimate posts with enthusiastic punctuation.
If spam filtering becomes a real goal rather than quick triage, compute the Breidbart index
during import — it is a counter, not a model — and use the subject classifier only to break
ties on the low-crosspost remainder.
