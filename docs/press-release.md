# PRESS RELEASE

---

**FOR IMMEDIATE RELEASE**

## Brands Can Now Find Real Influencers in Minutes — Not Weeks — Using Distributed PageRank at Scale

*New distributed computing system ranks 41 million social accounts by structural influence across a cluster of commodity laptops, exposing bot-inflated follower counts and surfacing the accounts that actually move audiences*

**SAN JOSE, Calif., May 2026** — Today, a research team at San José State University announced the availability of a distributed PageRank system purpose-built for influencer selection at scale. The system processes the full Twitter social graph — 41.6 million accounts and 1.47 billion follow relationships — across a cluster of eight commodity laptops connected over a standard Ethernet switch, converging to a definitive influence ranking in under 30 minutes.

---

### The Problem

Every year, brands spend billions of dollars on influencer marketing campaigns that underperform. The standard approach — sort candidates by follower count, pick the biggest numbers — is broken. Follower counts are trivially inflated: bots follow accounts en masse, engagement is purchased, and mega-accounts with 10 million followers routinely deliver less measurable impact than niche accounts with 200,000 genuine followers. Brands have no reliable way to tell the difference at scale, so they overpay for reach that does not convert.

Existing solutions either require expensive third-party SaaS platforms with opaque methodologies, or rely on single-machine tools that cannot process a full social graph without running for days. Neither option gives a brand a transparent, auditable, structurally grounded ranking they can trust.

---

### The Solution

The SJSU Distributed PageRank system solves this by applying the same algorithm that made Google's search results trustworthy to the influencer selection problem. PageRank scores an account highly only when it is followed by *other* well-followed accounts — a structural signal that is orders of magnitude harder to manufacture than a raw follower count. A bot farm can buy 100,000 followers overnight. It cannot manufacture the interlocking web of trust that drives a high PageRank score.

The system partitions the social graph across eight machines and runs the algorithm in parallel, with all communication over raw TCP sockets the team built from scratch. It converges in 30–60 iterations. When it finishes, a brand receives a ranked list of accounts ordered by structural influence — not vanity metrics.

*"We ran both rankings on the same graph,"* said the project lead. *"The top accounts by follower count and the top accounts by PageRank barely overlapped. The follower-count list was dominated by accounts that had clearly purchased engagement. The PageRank list surfaced accounts embedded in the real conversational core of the network — people other influential people actually follow. That is the list a brand wants."*

---

### Manipulation Resistance — Demonstrated

The team ran a controlled experiment: they injected 10,000 synthetic spam accounts into the graph. Each spam account followed every other spam account and followed a set of target real accounts — exactly the pattern used by commercial follower-inflation services.

The result was stark. The target accounts rocketed to the top of the follower-count ranking. They barely moved in the PageRank ranking. The spam followers had no structural weight — they were followed only by other spam accounts — so their endorsements carried negligible influence. The system surfaced the manipulation automatically, with no manual review.

---

### How It Works

A brand or data team provides the social graph as an edge list. A preprocessing script partitions the graph across eight machines in under 30 minutes. The cluster — eight laptops and a $35 Gigabit Ethernet switch — then runs PageRank to convergence using a bulk-synchronous parallel algorithm, with each machine owning roughly 5.2 million accounts and 183 million follow relationships. When the algorithm converges, the system outputs a ranked list of the top accounts by structural influence, with original platform IDs attached.

The entire pipeline, from raw graph to final influencer shortlist, runs on hardware a team already owns. No cloud subscription, no black-box vendor, no per-query pricing.

---

### Getting Started

The system is open source and available at the project repository. To run it:

1. Download the SNAP Twitter-2010 dataset (5.6 GB)
2. Run the preprocessor to generate partition files for each machine (~30 minutes)
3. Copy one partition file to each of the eight worker laptops
4. Start the coordinator, then start the eight workers
5. The system converges automatically and writes the ranked influencer list

Full setup instructions, hardware requirements, and the cluster configuration file are included in the repository.

---

### Frequently Asked Questions

**Q: Why eight machines? Can it run on fewer?**
The system is configurable. It runs correctly on a single machine (N=1) for development and testing, and scales to any number of workers. Eight machines was chosen to demonstrate meaningful parallel speedup while remaining practical for a university lab setting.

**Q: How long does it take to run?**
Preprocessing takes 15–30 minutes and is a one-time step. A full distributed run on the Twitter-2010 graph converges in 30–60 iterations. Per-iteration time depends on hardware and network speed; on Gigabit Ethernet with commodity laptops, total runtime is under 30 minutes.

**Q: What if a machine crashes mid-run?**
The current version uses a strict fault model: if any machine fails, the run aborts. Fault tolerance and dynamic worker membership are identified as the primary areas for future work.

**Q: How do the results compare to a single-machine reference implementation?**
The distributed results match a sequential reference implementation to within an L1 tolerance of 1×10⁻⁶ — verified on every run as part of the correctness test suite.

**Q: Does this work on graphs other than Twitter?**
Yes. The system accepts any directed edge list in the SNAP format. It has been validated on the SNAP Pokec social network dataset and designed to generalize to any large directed graph.

**Q: What makes this different from running PageRank in a framework like Spark or Flink?**
This system was built from first principles — raw TCP sockets, custom binary serialization, hand-written BSP iteration logic — specifically to characterize the performance and scaling behavior of distributed graph computation at the algorithm level, without the overhead and abstraction of a general-purpose framework. The result is a system whose behavior is fully transparent and whose communication-vs-computation tradeoffs are directly measurable.

---

*About the team: This system was built by a 13-member student team at San José State University as a capstone project, completing development in four weeks. The team is divided into four groups responsible for data preprocessing, the TCP networking layer, the coordinator process, and the distributed worker and PageRank core.*

---
