---
slug: postgres-fm-interview
title: 'Interview: Multigres on Postgres.FM'
authors: [sugu]
date: 2025-07-11
---

# Interview: Multigres on Postgres.FM

Sugu discusses Multigres on the Postgres.FM YouTube channel. 

<!--truncate-->

<iframe
  width="700"
  height="450"
  src="https://www.youtube-nocookie.com/embed/KOepJivmWTg"
  title="YouTube video"
  frameBorder="0"
  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
  allowFullScreen
></iframe>

## Chapters

- [00:00:00](https://www.youtube.com/watch?v=KOepJivmWTg) - Meet Sugu & Multigres
- [00:01:39](https://www.youtube.com/watch?v=KOepJivmWTg&t=99s) - Why Sharding Now?
- [00:03:20](https://www.youtube.com/watch?v=KOepJivmWTg&t=200s) - Timing is Everything
- [00:04:55](https://www.youtube.com/watch?v=KOepJivmWTg&t=295s) - Building Postgres-Native
- [00:06:26](https://www.youtube.com/watch?v=KOepJivmWTg&t=386s) - Go vs Rust Decision
- [00:08:56](https://www.youtube.com/watch?v=KOepJivmWTg&t=536s) - Local Storage Strategy
- [00:10:42](https://www.youtube.com/watch?v=KOepJivmWTg&t=642s) - MySQL to Postgres Port
- [00:12:18](https://www.youtube.com/watch?v=KOepJivmWTg&t=738s) - License Philosophy
- [00:14:18](https://www.youtube.com/watch?v=KOepJivmWTg&t=858s) - Apache vs BSD Choice
- [00:15:30](https://www.youtube.com/watch?v=KOepJivmWTg&t=930s) - RDS Compatibility Trade-offs
- [00:17:04](https://www.youtube.com/watch?v=KOepJivmWTg&t=1024s) - Managed-Only Approach
- [00:19:24](https://www.youtube.com/watch?v=KOepJivmWTg&t=1164s) - Learning from Vitess
- [00:21:34](https://www.youtube.com/watch?v=KOepJivmWTg&t=1294s) - Protection Before Sharding
- [00:23:20](https://www.youtube.com/watch?v=KOepJivmWTg&t=1400s) - Observability Built-in
- [00:24:52](https://www.youtube.com/watch?v=KOepJivmWTg&t=1492s) - OLTP vs OLAP Focus
- [00:26:42](https://www.youtube.com/watch?v=KOepJivmWTg&t=1602s) - YouTube's Scale Lessons
- [00:28:22](https://www.youtube.com/watch?v=KOepJivmWTg&t=1702s) - When to Start Sharding
- [00:30:16](https://www.youtube.com/watch?v=KOepJivmWTg&t=1816s) - Small Instances Win
- [00:31:52](https://www.youtube.com/watch?v=KOepJivmWTg&t=1912s) - Physical Replication Limits
- [00:33:00](https://www.youtube.com/watch?v=KOepJivmWTg&t=1980s) - Logical Replication Plans
- [00:35:12](https://www.youtube.com/watch?v=KOepJivmWTg&t=2112s) - Schema Change Handling
- [00:36:36](https://www.youtube.com/watch?v=KOepJivmWTg&t=2196s) - Sync Replication Problems
- [00:38:24](https://www.youtube.com/watch?v=KOepJivmWTg&t=2304s) - Data Loss Scenarios
- [00:40:12](https://www.youtube.com/watch?v=KOepJivmWTg&t=2412s) - Two-Phase Sync Solution
- [00:41:38](https://www.youtube.com/watch?v=KOepJivmWTg&t=2498s) - Beyond Raft Consensus
- [00:43:58](https://www.youtube.com/watch?v=KOepJivmWTg&t=2638s) - FlexPaxos Introduction
- [00:46:00](https://www.youtube.com/watch?v=KOepJivmWTg&t=2760s) - Durability Over Quorums
- [00:47:56](https://www.youtube.com/watch?v=KOepJivmWTg&t=2876s) - Wild Goose Chase Recovery
- [00:49:42](https://www.youtube.com/watch?v=KOepJivmWTg&t=2982s) - Distributed System Reality
- [00:51:36](https://www.youtube.com/watch?v=KOepJivmWTg&t=3096s) - Query Planner Decisions
- [00:53:18](https://www.youtube.com/watch?v=KOepJivmWTg&t=3198s) - Parser Compatibility
- [00:54:40](https://www.youtube.com/watch?v=KOepJivmWTg&t=3280s) - Function Routing Challenge
- [00:56:36](https://www.youtube.com/watch?v=KOepJivmWTg&t=3396s) - Select Function Writes
- [00:58:14](https://www.youtube.com/watch?v=KOepJivmWTg&t=3494s) - Aurora Global Inspiration
- [01:00:02](https://www.youtube.com/watch?v=KOepJivmWTg&t=3602s) - Cross-Shard Transactions
- [01:01:36](https://www.youtube.com/watch?v=KOepJivmWTg&t=3696s) - Materialized Views Magic
- [01:03:48](https://www.youtube.com/watch?v=KOepJivmWTg&t=3828s) - Reference Table Distribution
- [01:05:38](https://www.youtube.com/watch?v=KOepJivmWTg&t=3938s) - 2PC Performance Reality
- [01:07:20](https://www.youtube.com/watch?v=KOepJivmWTg&t=4040s) - Isolation Trade-offs
- [01:08:42](https://www.youtube.com/watch?v=KOepJivmWTg&t=4122s) - Distance Matters
- [01:10:42](https://www.youtube.com/watch?v=KOepJivmWTg&t=4242s) - Local Disk Advantages
- [01:13:54](https://www.youtube.com/watch?v=KOepJivmWTg&t=4434s) - Backup Recovery Speed
- [01:15:32](https://www.youtube.com/watch?v=KOepJivmWTg&t=4532s) - Edge Case Problems
- [01:17:00](https://www.youtube.com/watch?v=KOepJivmWTg&t=4620s) - Current Progress Update
- [01:18:20](https://www.youtube.com/watch?v=KOepJivmWTg&t=4700s) - Team Building Plans
- [01:19:14](https://www.youtube.com/watch?v=KOepJivmWTg&t=4754s) - Final Thoughts


## Transcription

{/*
docker pull ghcr.io/shun-liang/yt2doc

docker run ghcr.io/shun-liang/yt2doc --video "https://www.youtube.com/watch?v=KOepJivmWTg"
*/}

#### Meet Sugu & Multigres

Hello and welcome to Postgres FM, a weekly show to share about all things PostgreSQL. I am Nikolai Samokhvalov of Postgres AI, and I'm joined as usual by Michael Favaro. Hey Nick, hey Michael, and welcome all to our guests. Yeah, we are joined by a very special guest, Sugu, who is a co-creator of Vitess, co-founded PlanetScale, and is now at Supabase working on an exciting project called Multigres. So welcome Sugu. Thank you. Glad to be here.

Alright, it's our pleasure. So it's my job to ask you a couple of the easy questions to start off.

So what is Multigres and why are you working on it?

Multigres is a Vitess adaptation for Postgres. It's been on my mind for a long time, many years, and we even had a few false starts with this project. And I guess there is a timing for everything, and finally the timing has come. So I'm very excited to get started on this finally.

Yeah, timing is an interesting one. It feels like for many years I was looking at PlanetScale and Vitess specifically, very jealously, thinking you can promise the world, you can promise this, you know, horizontal scaling with a relational database for OLTP. And it's, you know, all of the things that people want, and we didn't really have a good answer for it in Postgres, but all of a sudden in the last few months, it seems almost, there are now three or four competing.

#### Why Sharding Now?

All Doing It, So Why Now, Why Is It All Happening Now?

About Vitess for Postgres - we started and a couple of times we had calls where I tried to involve a couple of guys, and from my understanding it never worked because people could not do it themselves being busy with, I guess, MySQL-related things, and guys looking at the complexity of Postgres and didn't proceed. And actually in one case they decided to build from scratch - it was a spectacular project, it's still alive and there is sharding for Postgres.

Yeah, and you borrowed and you borrowed and they borrowed it.

#### Timing is Everything

Yeah, so other folks were also involved, and so for me it was disappointing that it didn't work, and at some point I saw a message in Vitess, I think, that we are not going to do it, so like don't expect it. I felt so bad because I was so excited about doing it, and then I realized, oh my god, you know. But now PlanetScale started to support Postgres, so what's happening? I don't understand - just right time, right? Enough number of companies using Postgres which really needed it, at least one horse will win. So yeah, it's great, but yeah, long, long story to this point.

Yeah, sometimes when there are multiple projects there's kind of slight differences in philosophy or approach or trade-offs, like willing to trade one thing off in relation to another, and I saw your plan, I really liked that you mentioned building incrementally, so Vitess is a huge project, lots and lots of features, but I've heard you talking in the past about building it quite incrementally while at YouTube, you know, it didn't start off as complex as it is now, obviously, and you did it kind of one feature at a time, and it sounds like that's the plan again with Multigres, is that different to some of the other projects, or what do you see as your philosophy and how it might differ slightly to some of the others?

#### Building Postgres-Native

I think my philosophy is that I would say I don't want to compromise on what the final project is going to look like. Which is a project that should feel native as if it was for Postgres by Postgres kind of thing. I wanted to be a pure Postgres project. And Go definitely will bring a few hundred microseconds of latency overhead. Usually it's not a big deal, but maybe in some cases it's some deal, right? Are you happy with Go?

Yeah, because you're one of the first big Go language users building Vitess as we know from various interviews and so on. So it's still a good choice because now there is Rust, right?

#### Go vs Rust Decision

Yes, I would say, by the way, when we started, compared to where Go is today, it was a nightmare. Like 10 milliseconds or something round-trip is what we were paying for. Those days we had hard disks, by the way, so that's another 3-5 milliseconds just within the database. But things are a lot better now, and at this point, the way I would put it is, like the trade-offs are in favor of Go. Let's put it that way, mainly because there is a huge amount of existing code that can just lift and port. And rewriting all of that in Rust is going to just delay us. And at least in Vitess, it has proven itself to scale for like multi-hundreds of terabytes. And the latencies that people see are not affected by a couple of hundred microseconds. So I think plus there's this inherent acceptance of this network latency for storage and stuff. And if you bring the storage local, then this actually wins out over anything that's there.

That's exactly what I wanted to mention.

Yeah, I see PlanetScale right now. They came out with Postgres support, but no Vitess. I'm very curious how much it will take for them to bring it like in the competition with you. It's an interesting question, but from past week, I see my impression is like, my take is on local storage. And this is great because local storage for Postgres, we use it in some places where we struggle with EBS volumes and so on. But it's considered not standard, not safe, blah, blah. There are companies who use it. I know myself, right? And it's great. Today, for example, Patroni, and since Postgres 12, we don't need to restart nodes when we have failover. So if you lose a node, forget about the node, we just failover and so on. And with local storage, not a big deal. But now I expect with your plans to bring local storage, it will become more... I expect it will be more and more popular, and that's great. So you shave off latency there and keep going...

#### Local Storage Strategy

It's a win because one network hop has completely eliminated a language level overhead. It might be that Go will improve additionally but yeah good. I wanted to go back so you mentioned not wanting to compromise on feeling Postgres native and that feels to me like a really big statement coming from Vitess being very MySQL specific. Saying you want to be Postgres native feels like it adds a lot of work to the project or you know it it it feels like a lot to me. What is it - is it is that about compatibility with the project like what what does it mean to be Postgres native. There's two answers - one is why do we still think we can bring Vitess if it was built for MySQL and how do you make it Postgres native. That's because of Vitess's history - for the longest time Vitess was built not to be tied to MySQL, it was built to be a generic SQL 92 compliant database - that's actually that was actually our restriction for for a very long time until the MySQL community said you need us to you know you you need to support all these MySQL features otherwise we won't see it, right - common table expressions with... right it's yeah I guess it's SQL 99 feature not 92.

Yeah I think the first part that I built was SQL 92 which is the most popular one.

#### MySQL to Postgres Port

So, that's Answer 1. Answer 2 is more about the behavior of Postgres. What we want is to completely mimic the Postgres behavior right from the beginning. Basically, in other words, we plan to actually copy or translate what we can from the Postgres engine itself, where that behavior is very specific to Postgres. And the goal is not compatibility just at the communication layer, but even internally, possibly even recreating bugs at the risk of recreating bugs. In this case, it's very, so there are products, in the hands of Microsoft, it got everything open sourced, so before resharding was only in paid version now, it's in free version open sourced, so it's fully open sourced, and they put Postgres in between, so they don't need to mimic it, they can use it right. And latency overhead is surprisingly low, we checked it.

Well, let's see, but it's whole database in between, but it's sub millisecond, so it's acceptable as well. I think it's half millisecond overhead in our experiments with simple select 1 or something, select. So don't you think it's like in comparison, it's quite a challenging point, when you say I'm going to mimic a lot of stuff, but they just use Postgres in between.

#### License Philosophy

Yeah, I think there's a difference in architecture between, or approach between Multigres vs Citus. I think the main difference is it's a single coordinator for Citus and there is some bottleneck issue with that. If you scale to extremely large workloads like that goes into millions of QPS, hundreds of terabytes. So having that single bottleneck I think would be a problem. In the future, when I understand you can put multiple, like you can have multiple coordinator nodes there and also you can put a load balancer to mitigate the connection issues. So it can scale as well.

That's good, that's not that something I haven't known before. So it's possible then that they may also have something that can viable scales for OLTP. So we're still exploring this and more benchmarks are needed.

Actually I'm surprised how few and not comprehensive benchmarks are published for this.

Yeah, what I know of Citus is probably what you told me when we met. So it's about five years old.

Another big difference and this is typically Nikolai's question is on the license front. I think you picked about as open a license as you could possibly pick which is not the case I think for many of the other projects. So that feels to me like a very Supabase thing to do and also in line with what Postgres is, and that seems to me like a major advantage in terms of collaborating with others, other providers also adopting this or working with you to make it better - what's your philosophy on that side of things? My philosophy is my metric for success.

#### Apache vs BSD Choice

The only way to have a project widely adopted is to have a good license, a license that people are confident to use. That has been the case from day one of Vitess. We actually first launched a BSD license, which is even more permissive than Apache. Why do they say it? Do you know? Why CNCF wants Apache?

I think Apache is a pretty good license. They just made it a policy. I mean, had we asked to keep the BSD license, they would have allowed us, but we didn't feel like it was a problem to move to Apache. I remember you described when you did it at YouTube, you thought about external users. You need external users for this project. And I guess at Google, GPL is not popular at all, we know.

#### RDS Compatibility Trade-offs

Also, compared to Citus, I think you have chances to be compatible with RDS and other Managed Postgres, to work on top of them, right? Unlike Citus, which requires extensions and so on, right?

Correct, correct, yes. This was actually something that we learnt very early on. Wanting to work, like, we made a five-line change on MySQL. Just to make Vitess work initially, and it was such a nightmare to keep that binary up, to keep that build running fork. And, yeah, to keep that fork alive. So we decided, no, it's like, we are going to make this work without a single line of code change in MySQL, and that actually is what helped us move forward, because people would come in with all kinds of configurations and say, you know, make it work for this.

So in this case, actually, we will probably talk about the consensus part, that is one part that we think it is worth making a patch for Postgres, and we're going to work hard at getting that patch accepted. But I think what we will do is, we will also make Multigres work for unpatched Postgres, for those who want it that way, except they will lose all the cool things about what consensus can give you. I'm smiling because we have so many variations.

#### Managed-Only Approach

This sync might happen as well. Don't they claim full compatibility with Postgres? Not fully, but most of it. They did interesting stuff in memory, like column storage in memory for tables. It's row storage on disk, but column storage in memory. But it looks like kind of Postgres and we actually even had to get some questions answered from my team unexpectedly because we don't normally work with OLAP. But it looks like Postgres. So I could imagine the request, let's support OLAP as well.

But my question, I remember featuring Vitess, we work with RDS and managed MySQL. Did this feature, has this feature survived?

No, actually later we decided that at least we call it actually managed versus unmanaged. Managed meaning that Vitess manages its databases. And unmanaged means that the database is managed by somebody else with just access and proxy to serve queries.

At some point in time, we realized that supporting both is diluting our efforts. And that's when we decided it's not worth it to try and make this work with every available version that exists out there in the world. And we said, okay, we will do only managed, which means that we will manage it ourselves. And if you want, we will build the tools to migrate out of wherever you are. And we'll make it safe, we'll make it completely transparent. In other words, you deploy with us on both and then we'll migrate your data out without you having to change your application. But then

#### Learning from Vitess

Vitess can be more intentional about its features, more opinionated about how clusters are to be managed, and we were able to commit to that because at that point Vitess had become mature enough people were completely trusting it. They actually preferred it over previous other managed solutions, so it wasn't a problem at that time.

Yeah, five-nines is like what Vitess shoots for, and like most big companies that run Vitess do operate at that level of availability with this team.

So what's the plan for Multigres? I go into support, so not only managed version, right?

Yes, it would be only managed versions because I believe that the cluster management section of Vitess will port directly over to Postgres, which means that once it goes live, it will be coming with batteries included on cluster management, which should hopefully be equal to or better than what is already out there. So I don't see a reason why we should try to make it work with everything that exists today. So it means this is the same with Citos, it doesn't work with Vardias on one hand, but on another hand I don't see it's only a sharding solution, it's everything which is great. I mean it's interesting, super interesting. A lot of problems will be solved, and I expect even more managed services will be created. I don't know how it will continue, like in terms of super bits, because very open license and so on, but also I expect that many people will think we consider their opinion about managed, we have that episode about this. This is my usual opinion about managed services because we hired super user from you, we don't have your access.

#### Protection Before Sharding

It's hard to troubleshoot problems. In this case, if problems are solved with this, and this gives you a new way to run Postgres, so if many problems are solved, it's great. Right? If you want to solve this.

Yeah, if you may not know, the initial focus of Vitess was actually solving these problems first. Sharding actually came much later. Protecting the database, making sure that they survive abusive queries. Basically, that's what we built Vitess for initially. And the counterpart of taking away power from the user, like you said, is one is, well, we now know exactly how to make sure that the cluster doesn't go down. And two, we countered that by building really, really good metrics. So when there is an outage, you can very quickly zero in on a query. If a query was responsible, Vitess will have it on top of, like on the top of the list.

I'm saying that this is a query that's killing your database. So we build some really, really good metrics, and which should become available in multigress, probably from day one. That's interesting. I didn't see, maybe I missed, I didn't see in the read me, you were writing right now in the project. There's a last section called observability. I missed that. We're actually building something there as well. I for a regular pause, because I have a very curious, I will definitely revisit this interesting. So yeah, great. And yeah, also quite a big, I feel like this is quite a big difference on the, at least with Citus in terms of the philosophy, or at least the origin story. I feel like that started.

#### Observability Built-in

Added much more with OLAP-focused features in terms of distributed queries and parallelized across multiple shards and aggregations and columnar, and loads of things that really benefit OLAP workloads, whereas this has come from a philosophy of, let's not worry about optimizing for those cross-shard queries, this is much more, let's optimize for the single-shard, very short, quick OLTP queries, and let's make sure we protect it against abuse of query. So it feels like it's coming, the architecture, it's coming from a very different place of what to optimize for the first. And historically, that was YouTube's problem, surviving the onslaught of a huge number of QPS, and making sure that one single QPS doesn't take, you know, the rest of the site down. Yeah, perfect, it makes loads of sense. So actually before we move on too much from that, where do you see sharding as becoming necessary? Is it just a case of a total number of QPS, or like rights per second type, we've talked about sharding in the past and talked about kind of a max that you can scale up to, perhaps in terms of rights, in terms of wall, the wall per second I think was the metric we ended up discussing. Are there other reasons, or kind of bottlenecks that you see people getting to that sharding then kind of make...

#### OLTP vs OLAP Focus

What makes sense is now time where you should be considering this point? Well, there is a physical limiting factor which is the single, if you max out your single machine, that is your Postgres server, then that's the end of your scale. There is nothing more to do beyond that. And there are a lot of people already heating those limits from what are here. And the sad part of it is they probably don't realize it. As soon as that limit is hit, in order to protect the database, they actually push back on engineering features. Indirectly, saying that, you know, this data, can you make it smaller, can you just sum all over the QPS or could you put it elsewhere?

Let's stop showing this number on front page and so on.

Yeah, and it affects the entire organization. It's a very subtle change, but the entire organization slows down. Like we experienced that at YouTube, when we were at our limits, the default answer from a DBA was always no. We used to even kid know, the answer is no, what's your question?

And when we started sharding, it took us a while to change our answer to say that, you know, bring your data like we can scale as much as you want. Believe it or not, we went from 16 shards to 256 in no time. And the number of features in YouTube exploded during that time because there was just no restriction on how much data you wanted to put. And coming back here, the upper like reaching the limit of a machine is actually something you should never do. It's very unhealthy for a large number of reasons.

#### YouTube's Scale Lessons

Like, even if there is a crash, how long is it going to take to recover? Like, the thing that we found out is once you can shard, it actually makes sense to keep your instances way, way, small. So, we used to run like 20 to 50 instances of MySQLs per machine, and that was a lot healthier than running big ones. For a couple of reasons, one is if you try to run so many threads within a process, that itself is a huge overhead for the machine. And it doesn't do that very efficiently, whereas it does it better if you run it as smaller instances. I think it's more of a feeling, but I don't know if there is proof or whatever. But in, like, go for example, wouldn't do well. Go, I think, beyond a certain memory size, or beyond a certain number of go routines would start to slow down, would not be as efficient as it was before. Mainly because the data structures to keep track of those threads and stuff, they are getting, they are growing bigger. But more importantly, on an outage, a smaller number of users are affected. If you have 256 shards and one shard goes down, it is 1,256th of the outage. And so, the site looks a lot healthier, behaves a lot healthier, there's less panic if a shard goes down. So, people are, you know, a lot less stressed managing such instances. Right, I wanted to mention that this discussion was with Lev Kukotov.

#### When to Start Sharding

In previous conversations, competitors, new sharding solutions written in Rust, we discussed that there is a big limitation when Postgres - so replication, physical replication has limitation, because it's single threaded process on standby. If we reach like, something like, 150, 200, 250 megabytes per second, depending on core, and also number of, not number, structure, we hit one single CPU, 100% one process, and it becomes bottlenecked, and replication standbys, they start lagging. It's a big nightmare, because you usually buy data, but it's high scale, you have multiple replicas, and you offload a lot of read-only queries there, and then you don't know what to do except as you describe, let's remove this feature and slow down development, and this is not fun at all. So, what I'm trying to do here, is trying to move us to discussion of replication, not physical, but logical. I noticed your plans involved heavy logical replication in Postgres, but we know it's improving every year. So, when we started the discussion, 5, 6 years ago, it wasn't much worse, right now it's much better, many things are solved, improved, but many things still are not solved. For example, schema changes are not replicated. And sequences, there is work in progress, but if it's committed, it will be only in Postgres 19, not in 18, so, it means, like, long wait for many people. So, what are your plans here, are you ready to deal with problems like this? In Postgres, pure Postgres problems, you know? Yeah! Yes! Yes! Ah! How did you ask me, everything?

#### Small Instances Win

I think the Postgres Problems are less than what we faced with my sequel. I wanted to involve Physical as well because this great talk by Kokushkin which describes very bad anomalies when data loss happens and so on.

Yeah, let's talk about this. Yeah, we should talk about both. I think overall the Postgres design is cleaner, is what I would say. Like you can feel that from things. Like the design somewhat supersedes performance which I think in my case is a good trade-off especially for sharded solutions because some of these design decisions affect you only if you are running out. If you are pushing it really, really hard then this design decisions affect you but if your instances are small to medium size, you won't even know and then you benefit from the fact that these designs are good. So I actually like the approaches that Postgres are taken with respect to the wall as well as logical replication and by the way I think logical replication theoretically can do better things than what it does now and we should push those limits.

But yes, I think the issue about schema not being as part of logical replication, it feels like that is also a theoretically soluble problem except that people haven't gotten to it. I think there are issues about the transactionality of details which doesn't even exist in my sequel so at least in Postgres it's like it exists in most cases there are only a few cases where

#### Physical Replication Limits

We don't want you to get the wrong impression, we'll let you do it non-transactionally, and we know that it's non-transactionally, and therefore we can do something about it. Those abilities don't exist previously. But eventually, if it becomes transactional, then we can actually include it in a transaction.

Yeah, just for those who are curious, because there is, there is, like, concept, all-DDL-in-positive systems. Actually, here we talk about things like creating this concurrently, because we had discussion offline about this before recording. So yeah, creating this concurrently can be an issue, but you obviously have a solution for it. That's great. The way I would say it is, we have dealt with much worse with MySQL, so this is much better than what was there then. Sounds good.

#### Logical Replication Plans

This is an interesting talk by Kukushkin. He presented it recently on one conference by Microsoft, describing that synchronization in Postgres is not what you think. Correct. Correct. What are you going to do about this? Well, I was just chatting with someone. And essentially, synchronous replication is theoretically impure when it comes to consensus. I think it's provable. But if you use synchronous replication, then you will hit corner cases that you can't handle. And the most egregious situation is that it can lead to some level of definitely split brain. But in some cases, it can even lead to downstream issues. Because it's a leaky implementation. There are situations where you can see a transaction and think that it is committed. Later, the system may fail and in the recovery, you may choose not to propagate that transaction or may not be able to. And it's going to discard that transaction and move forward. But this is same as with synchronous replication, it's the same.

We're just losing some data, right?

It is same as asynchronous replication. It's just the data loss. It's data loss.

Correct. It's data loss. But for example, if you're running a logical replication of one of those, then that logical replication may actually propagate it into an external system. And now you have a corrupted downstream system.

#### Schema Change Handling

So, those risks exist, and at Vitess scale people see this all the time, for example, and they have to build defenses against this, and it's very, very painful. It's not impossible, but it's very hard to reason about failures when a system is behaving like this. So, that is the problem with synchronous replication. And this is the reason why I feel like it may be worth patching the post-cress, because there is no existing primitive in-post-cress on which it can build a clean consensus system. I feel like that primitive should be in-post-cress.

I now remember from Kukushkin's talk, there is another case when on primary transaction looks like not committed, because we wait replica, but replica somehow lost connection or something, and when we suddenly, and client thinks it's not committed, because the commit was not returned, but then it suddenly looks committed. It's like not data loss, it's data un-loss somehow, suddenly, and this is not all right as well. And when you think about consensus, I think it's a very good describing these things, like concept and distributed systems, it feels like if you have two places to ride, definitely there will be corner cases where...

#### Sync Replication Problems

We'll go off if you don't use two-Phase Commit, right? And here we have this, but when you say you're going to bring something with consensus, it immediately triggers my memory, how difficult it is and how many attempts it was made to bring pure Echa to Postgres, just to have auto-failure, all of them failed, all of them. And let's be outside of Postgres. So here, maybe it will be similar complexity to bring these two inside Postgres. Is it possible to build outside this thing? It is not possible to build it outside, because if it was, that is what I would have proposed. The reason is because building it outside is like putting bandaid over the problem. It will not solve the core problem. The core problem is you've committed data in one place and if that data can be lost and there is a gap when the data can be read by someone, causes is the root cause of that problem, that is unsolvable, even if you later raft may choose to honor that transaction or not, and that becomes ambiguous, but we don't want ambiguity.

What if we create something extension to commit, make it extendable to talk to some external staff to understand that committed can be finalized or something? I don't know, consensus will bring. Correct, correct. So essentially if you reason to about this, your answer will become a two-phase system.

Yeah, without a two-phase system. But as I told you, two-phase commit in all TPP world, Postgres all TPP world, consider to read.

#### Data Loss Scenarios

It's a really slow and the rule is less than just avoided. I see your enthusiasm, and I think I couldn't find good benchmarks, zero, published. This is not two-Phase Commit, by the way. This is two-Phase Synchronization. I understand, it's not the two-Phase Commit, it's more communication happens. I understand. So, two-Phase Synchronization, the network overhead is exactly the same as full sync. Because the transaction completes on the first sync. Later, it sends an acknowledgement saying that, yes, I'm happy you can commit it. But the transaction completes on the first sync. So, it will be no worse than full sync.

Yeah, compared to current situation, when primary commit happens, but there is a lock, which is being held until... It is the same custom. We wait until standby. And for user, it looks like a lock is released, it thinks, okay, commit happens. But the problem with this design, if, for example, standby starts, lock is automatically released and commit is here, and it's unexpected. This is a data-unloss, right? So, you are saying we can redesign this network cost will be the same, but it will be pure.

Yeah, that's great. I like this. I'm just thinking, will it be acceptable? Because bringing out a failover is not acceptable. There was another attempt last year from someone. And with great enthusiasm, let's bring out a failover inside Postgres. Actually, maybe you know this guy, it was Constantine Osipov, who built a rental database system. It's like a memory. He was X-MySQL in performance. After X-MySQL, Osipov was my SQL.

#### Two-Phase Sync Solution

Let's build this, great enthusiasm, but it's extremely hard to convince such big thing to be in core. So, if you say it's not big thing, this already? So, I'll probably have to explain it in a bigger blog. But essentially, now that I've studied the problem well enough, the reason why it's hard to implement consensus is that it's not a big thing. In Postgres, with the wall, is because they are trying to make Raft work with wall, and there are limitations about how the Raft, how commits work within Postgres, that mismatch with how Raft wants commits to be processed, and that mismatch so far I have not found a way to work around that. But a variation of Raft can be made to work. Interesting. I don't know if you know about my blog series that I wrote when I was at Planet Scale, it's an eight-part block series about generalized consensus. People think that Raft is the only way to do consensus, but it is one of a thousand ways to do consensus. So, that block series explains the rules you must follow if you have to build a consensus system.

#### Beyond Raft Consensus

If you follow those rules, you will get all the properties that are required by a Consensus System. So, this one that I have, the design that I have in mind follows those rules and I am able to prove to myself that it will work but it's not Raft. It's going to be similar to Raft. I think we can make Raft also work but that may require changes to the wall which I don't want to do. So, this system I want to implement without changes to the wall, as possibly a plugin.

Well, now I understand why you, like, another reason you cannot take Patroni, not only because it's Python versus Postgres but also because you need another version of Consensus Algorithm. Correct, correct. And among those hundred thousand millions of ways. By the way, Patroni can take this and use it because it's very close to how full thing works.

I was just thinking, watching Alexander Kukushkin's talk, he said a couple of things were interesting. One is that he was surprised that this hasn't happened upstream. So, you definitely have an ally in Kukushkin in terms of trying to get this up streamed but also that he thinks every cloud provider has had to pass to in order to offer their own high availability products with Postgres. Each one has had to patch it and they are having to, or you mentioned earlier today, how painful it is to maintain even a small patch on something. I don't think it's every, I think it's Microsoft for sure knowing where Kukushkin works at. But maybe more, not everybody. All I mean is that there are growing number of committers working for hyperscale and hosting providers. So, I suspect you might have more optimism for Consensus or at least a few allies in terms of getting something committed upstream. So, I personally think there might be growing chance of this happening even if it hasn't in the past for some reason.

Yeah, I feel like also being new to the Postgres community, I am feeling a little shy about proposing this.

#### FlexPaxos Introduction

So, what I am thinking of doing is at least show it working. Have people gain confidence that, no, this is actually efficient and performant and safe. It's actually very hard to configure if your needs are different, which actually FlexPaxos does handle. It's actually something I'm co-inventor of of some sort.

And this block post. I don't hear the name, that's it. Can you explain it? It will be not super interesting.

Oh sure, yeah, so actually let me explain what is the reason why, so FlexPaxos was published a few years ago, about seven years ago or so. And if you see my name mentioned there, which I feel very proud of. And this block series that I wrote is actually a refinement of FlexPaxos. And that actually explains better why these things are important. The reason why it's important is because people think of consensus as either a bunch of nodes agreeing on a value. That's what you commonly hear. Or you think of reaching majority, reaching core on is important. But the true reason for consensus is just durability. When you ask for a commit and the system says, yes, I have it. You don't want the system to lose it. So instead of defining core and all those things, define the problem as it is and solve it the way it was asked for is, how do you solve the problem of durability in a transactional system?

#### Durability Over Quorums

The simple answer to that is, make sure your data is elsewhere. If there is a failure, your challenge is to find out where the data is and continue from where it went. That is all that consensus is about. Then all you have to do is have rules to make sure that these properties are preserved. Raft is only just one way to do this. If you look at this problem, if you approach this problem this way, you could ask for something like, I just want my data to go across availability zones. As long as it's in a different availability zone, I'm happy. Or you can say, I want the data to be across regions. Or I want at least two other nodes to have it. So that's your durability requirement. But you could say, I want two other nodes to have it, but I want to run seven nodes in the system or 20 nodes.

It sounds outrageous. But it is actually very practical in YouTube. We had 70 replicas. But only one node, the data have to be in one other node for it to be durable. And we were able to run this at scale. The trade off is that when you do a failover, you have a wild goose chase looking for the transaction that went elsewhere. But you find it and then you continue. So that is basically the principle of this consensus system. And that's what I want to bring in multigress. While making sure that the people that want simpler majority base forums to also work using the same primitives. Just quickly to clarify it, when you say the wild.

#### Wild Goose Chase Recovery

It's the same one, but you have to know which one that is. There was a time when we found that transaction in a different country, so we had to bring it back home and then continue. It was once it happened in whatever the 10 years that we ran. It's interesting that talking about Sharding, we need to discuss these things, which are not Sharding per se, right? It's about a chain inside each Shard, right? It's actually what I would call healthy database principles, which is, I think, somewhat more important than Sharding. It is true that it is to do with it being a distributed system, and that is because it's Sharded, right? I think they are orthogonal.

Yeah, I think Sharding, you can do Sharding on anything, right? You can do Sharding on RDS, somebody asked me, what about Neon? I said, you can do Sharding on Neon too, you put a proxy in front.

#### Distributed System Reality

The problem with Sharding is it is not just a proxy. That's what people think of it when they first think of the problem because they haven't looked ahead. Once you have Sharded, you have to evolve. You start with 4 Shards, then you have to go to 8 Shards. At some point of time, it changes because your Sharding Scheme itself will not scale. Like if you for example are in a multi-tenant workload and you say Shard by tenant. At some point of time, a single tenant is going to be so big that they won't fit in an instance and that we have seen. At that time, you have to change the Sharding Scheme.

So how do you change the Sharding Scheme?

Slack had to go through this where they were a tenant based Sharding Scheme and a single tenant just became too big. They couldn't even fit one tenant in one Shard. So they had to change their Sharding Scheme to be user based. They actually talk about it in one of their presentations. And Vitesse has the tools to do these changes without actually using you incurring any kind of downtime which again multi-grace will have.

I keep talking about Vitesse but these are all things that multi-grace will have which means that your future proved when it comes to and these are extremely difficult problems to solve. Because when you are talking about changing the Sharding Scheme, you are basically looking at a full crisscross replication of data. And across data centers.

Yeah, and also I know Vitesse version 3, right? It was when you...

#### Query Planner Decisions

We've changed, basically created a new planner to deal with arbitrary query and understand how to route it properly and where to execute it. Is it a single shard or it's global or it's different shards and so on? Are you going to do the same with Postgres? I think yes, right?

So that's the part that I'm still on the fence.

That, by the way, the V3 now has become Gen 4, it's actually much better than what it was when I built it.

The problem with V3 is that it is still not a full query. It doesn't support the full query set yet. It controls supports like 90% of it, I would say, but not everything.

On the temptations side, there's the Postgres engine that supports everything.

So I'm still debating how do we bring the two together?

If it was possible to do in a simple git merge, I would do it. But obviously this one isn't C, this was in Go.

And the part that I'm trying to figure out is how much of the sharding bias exists in the current engine in VTS?

If we brought the Postgres engine as is, without the sharding bias, would this engine work well for a sharded system?

So this looks like side of storage if you bring the whole Postgres.

There's a library, LibPigy Query, my locust fetal, which is...

#### Parser Compatibility

It takes the parser part of Posgas and brings it, and there is a Go version of it as well. So, I mean, don't talk on top of it.

Yes, it also uses it in the last version.

Is it like 100% Postgres Compatible?

Well, it's based on Postgres Source code. So, parser is fully brought, but it's not whole postgres.

So, maybe you should consider this.

If you're thinking about parsing, I mean queries and so on, but I'm very curious.

I also noticed you mentioned routing. It's only queries routed to replicas automatically, and this concerns me a lot because many Postgres developers, I mean, who use it?

Users. They use PL-produced PL functions, all PL Python functions and anything, which are writing data, and the standard way to call function is select.

Select Function Name.

#### Function Routing Challenge

So, understanding that this function is actually writing data is not trivial, right? And we know in PG-Pool, which I hold my life, I just avoid, I touched it a few times, decided not to use it all because it tries to do a lot of stuff at once, and always considered, like, no, I'm not going to use this tool. So, PG-Pool solves it, like, saying, okay, like, let's build a list of functions which are actually writing. Or something like this. So, it's like patch approach, you know, work around approach. So, this is going to be a huge challenge, I think, if you, for automatic routing, it's a huge challenge.

Yeah, I think this is the reason why I think it is important to have the full Postgres Functional Engine in Multigress, because then these things will work as intended, is my hope. What we will have to do is add our own shard at understanding to these functions, and figure, oh, what does it mean to call this function, right? If this function is going to call out to a different shard, then that interpretation has to happen at the higher level. But if that function is going to be accessing something within shard, then push the whole thing down and just let the push the whole select along with the function down and let the individual Postgres instance do it. Yeah, but how to understand function can contain another function, and so on, it can be so complex in some cases. Yeah. It's also funny, but there is still, there is actually Google Cloud SQL supports it, like, kind of language, it's not language called PL proxy, which is sharding for those who have workload only in functions. This can route to...

#### Select Function Writes

Welcome to PL/pgSQL - it still exists, but not super popular these days. But there is a big requirement to write everything in functions. In your case, if you continue, like, I have to expect in some case you would say, okay, don't use functions, but I'm afraid it's not possible, like, I love functions. Actually, Supabase loves functions because they use PostgREST, right? PostgREST like, it provokes you to use functions. Oh, really? Oh, yeah, yeah. Actually, I saw that, yeah. But in Vitess, I feel like this was a mistake that we made, which is if we felt that anything, any functionality that you used, didn't make sense. If I were you, I wouldn't do this, right? Because it's not, it won't scale, it's a bad idea, you know, it's like, those we didn't support. We didn't want to support. We said, no, we'll never do this for you, because, you know, we'll not give you a rope long enough to hang yourself. Basically, that was our philosophy. But in the new, in Multigres, we want to move away from that, which means that if you want to call a function that writes, have at it. Just put a comment, it's going to write something. Yeah, if you like, I don't know, if you want a function that calls a function that writes, have at it, we, if we cannot, like, the worst case scenario for us is, we don't know how to optimize this. And what we'll do is, we'll execute the whole thing on the coordinator.

#### Aurora Global Inspiration

There is another interesting solution in AWS or DS Proxy which, as I know it may be I'm wrong, when they needed to create a global, I think Aurora Global Database maybe or something like this. So there is a secondary cluster living in a different region and it's purely redone but it accepts rights. It comes, this proxy routes it to original primary, waits until this right is propagated back to replica and response.

Oh wow! I don't think that feature can be supported.

No, it's just some exotic, interesting solution I just wanted to share. Maybe, you know, if we, for example, if you originally route right to a replica then somehow in post this you understand oh it's actually right.

Yeah, so maybe 100% is theoretically impossible to support.

Yes, it's super exotic, okay. But I think if people are doing like that, doing things like that it means that they are trying to solve a problem that doesn't have a good existing solution.

Exactly. So if we can find a good existing solution I think they'll be very happy to adopt that instead of whatever they were trying to do.

Well, this is just multi-regions set up and I saw not one city or which wanted it like dealing with Postgres like say we are still single-region. We need to be present in multiple regions in case if one AWS region is down.

Right, it's also over here.

#### Cross-Shard Transactions

Yeah, so we will availability and the business characteristics. So, yeah. Anyway. Okay. Yeah, it's exotic. But, but interesting still. Yeah. So you've got a lot of work ahead of you, Sergey. I feel like we, we barely covered like one of so many topics. Let's touch something else. Like, maybe it's very little, but it's a lot of work ahead of you, Sergey. I feel like we barely covered like one of so many topics. Let's touch something else. Maybe it's very little. It's a long episode, but it's worth it, I think. It's super interesting. What else? What else? I think the other interesting one would be 2PC and Isolation. Hmm. Isolation from what? Like, the one issue with the Sharded Solution is that, again, this is a philosophy for the longest time in the test. We didn't allow 2PC. You said you shard it in such a way that you do not have distributed transactions. And many people lived with that. And some people actually let me interrupt. Let me interrupt you here because this is a, this is like the most the best feature I liked about the test. It's this materialized feature when data is brought. Oh, yeah, materialized is another time. That's actually a better topic than 2PC. Well, well, yeah, because this is your strength, right? So this is like, I love this idea. Basically, distribute it. The materialized view, which is...

#### Materialized Views Magic

Incremental Update,

That's great, we need it in Postgres ecosystem, maybe as a separate projective, we lack it everywhere, so yeah, this is how you avoid the distributed transactions basically, right?

No, this is one way to avoid it. There are two use cases where materialized views are super awesome. You know the table that has multiple foreign keys, but that has foreign keys to two different tables, is the classic use case, where the example I gave was a user that's producing music and listeners that are listening to it, which means that the row where I listen to this music has two foreign keys, one to the creator and one to the listener. And where should this role live, should this role live with the creator or should this role live with the listener is a classic problem and there is no perfect solution for it, it depends on your traffic pattern. But what if the traffic pattern is one way in one case and another way in another case, there is no perfect solution.

So, this is where in multi guess what you could do is you say, okay, in most cases this row should live with the creator, let's assume that, right?

So, then you say this row lives with the creator and we shard it this way, which means that if you join the creator table with this event row, it'll be all local joins. But if you join the listener's table with this event row, it's a huge cross-shard while Google's chase. So, in this case, you can say materialize this table using a different foreign key, which is the listeners foreign key, into the same shard at database as a different table name. And now you can do a local join with the listener and this event table. And this materialized view is near real time, basically the time it takes to read the wall and apply it. And this can go on forever. And this is actually also the seek.

#### Reference Table Distribution

Distributed Behind Re-Sharding, Changing the Sharding Key, This is essentially a table that has real-time presenting with two Sharding keys. If you say, oh, at some point of time, this is more authoritative, all you have to do is swap this out. Make one the source, the other is the target, you change your Sharding Key. Actually, the change Sharding Key works exactly like this for a table. This is the built-in generalization technique. This is what it works for. Yeah. Yeah. Exactly. And the other use case is when you re-shard, you leave behind smaller tables. Reference tables, we call them. And they have to live in a different database because they are too small. And even if you shard them, they won't shard well. Like, if you have, you know, a billion rows in one table and, you know, 1000 rows in a smaller table, you don't want to shard your 1000 row table. And there is no benefit to sharding that either. So, it's better that that table lives in a separate database. But if you want to join between these two, how do you do it? Right? The only way you join is to join at the application level, or read one and then read the other. And so at high QPS, it's not efficient.

So, what we can do is actually materialize this table on all the shards as reference. Yeah. And then all joins become, become local. Yeah. And you definitely need logical application for all these. So, this is where we started, like, challenges with logical application.

Yeah. Yeah. Great. You do have the, so the reason why two PC is still important, because there are trade offs to this solution, which is, there's a lag. So, it is, it takes time for the things to go to the

#### 2PC Performance Reality

2PC is essentially basically the transaction system itself trying to complete a transaction which means that it will handle cases where there are race conditions, right? If somebody else tries to change that role elsewhere while this role is being changed, 2PC will block that from happening whereas in the other case you cannot do that. There will be some video like on YouTube, we can say, okay, there will be some lag, probably some small mistake it's fine but if it's financial data it should be 2PC but latency of right will be high, throughput will be low, right?

This is... I actually want to... I read the design of... which is again, by the way, very elegant API and I assume I can see the implementation on the API and I don't think we will see performance problems with 2PC. We need to benchmark it. We will benchmark it but I will be very surprised.

I think there are some isolation issues that we may not have time to go through today because it's a long topic. Like the way 2PC is currently supported in Postgres, I think it will perform really well. The isolation issues when we sit in a read committed and use 2PC, you mean this, right?

Not an repeatable read. The read committed I think will be... there will be some trade-offs on read committed but not the kind that will affect most applications. MVCC will be the bigger challenge.

#### Isolation Trade-offs

What they hear is, most people don't use, like, the most common use case is lead committed. Of course, as default, yeah, as fast as default, yeah, so people won't even, yeah, I don't, I think this is already on some, they're already in bad state, it won't be worse. It won't be worse, yes.

Yes, yeah. As for 2PC, it of course depends on the distance between nodes, right? A lot, like if they are far they need to, we need to talk, like, client is somewhere, two nodes are somewhere, and if it's different, it will build the zones, it depends, right?

So this distance is, it's a big contributor to latency, right?

Network, because there are four communication messages that are needed. So, correct, correct. Actually, you can, I have actually the mathematics for it, but you're probably right, it's about double the number of round trips.

Yeah, if you put everything in one AZ, client and both, both primaries, we are fine, but it's, yeah, in reality, if they will be in different places, and if it's different regions, it's not there, of course, but at least, yeah, there are two PC is not done by the client, by the way, the two PC would be done by the VT gate, which would be, it should have the nodes.

#### Distance Matters

The Availability Zone is only for durability for replica level, but a two-PC coordinating between two primaries, which may actually be on the same machine for all your care. Imagine a real practical case, every shard has primary and a couple of standby. So are you saying that we need to keep primaries all in the same availability zone? That's usually how things are.

Interesting, I didn't know about this. I wanted to rattle a little bit about Planescale Benchmarks last week. They compare to everyone. It's not like I'm sorry, I will take a little bit of time. They compare to everyone and they just publish Planescale versus something. And this is very topic.

On charts, we have Planescale in single AZ, everything client and server in the same AZ. And line, which is normal case, client is in different AZ. And line with the same AZ is active, line is normal, not active. And others, like neon, super bass, everyone, it's different. And of course Planescale looks really well, because by default they presented numbers for the same availability zone. And below like the chart, everything is explained, but who reads it, right? So people just see the graphs. And you can unselect, select proper Planescale numbers and see that they are similar. But by default, same AZ numbers.

#### Local Disk Advantages

This is like benchmarking. If you look at the architecture, even fair comparison planets should come out ahead, like the performance of a local disk, of course, should. But this was Select 1, Disk 1 is not a benchmark. Well, it was part of benchmark, it's just checking query path, but it fully depends on where client and server are located.

So, what's the point showing better numbers just putting client closer? I don't like that part of that benchmark. Also, I saw the publications, but I didn't go into the details because it has to be faster because it's on local disks. For data which is not full in cache, of course, local disks are amazing. You're right, if the data is in cache, then all performance of everything would be the same.

Yeah, well, I wanted to share this, I wasn't knowing about this. But I fully support the idea of local disks, it's great. I think we need to use them more and more systems. I think I wouldn't be surprised if you reached out to planet scale. If you want to run your benchmarks, they may be willing to give you the...

This was called published, and in general, benchmarks look great, the idea is great. And actually, with local disks, the only concern is usually the limit, hard limit. We cannot have more space, but if we have started solution, there is no such limit. But speaking about the hard limit, today's SSDs, you can buy 100 plus terabytes SSD, single SSD, and you can probably stack them up on the next together. But about... I saw AWS SSD over 100 terabytes. In Google Cloud, 72 terabytes is hard limit for Z3 metal, and I didn't see more. So 72 terabytes, it's a lot, but sometimes it's already... At that limit, your storage is not the limit, you will not be able to run a database of that size on a single machine. Why not? We have cases CPU. Well, again, this problem will be replication. If we talk about single node, we can...

Or replication. 360 cores in the AWS almost 1000 cores already for Z1 scaleable, generation 5 or something. So hundreds of cores. Well, the problem is supposed to be design. If replication, physical replication was multi-threaded, we could scale more. By the way, replication is not the only problem. Backup recovery. If your machine goes down, you are down for hours.

#### Backup Recovery Speed

[Discussion about backup and restore speeds] Always saying that one terabyte per hour is what you should achieve for restore, if it's below, it's bad. Now I think one terabyte per hour is already not enough. Yes, yes. So with the best EBS volumes, we managed to achieve, I think, seven terabytes per hour to restore with WAL-G. And that's great. The greatest danger there, you could become a noisy neighbor. So we actually built throttling in our restore, just to prevent being noisy neighbors. With local disks, you lose the ability to use EBS snapshots, cloud disk snapshots. Correct, correct. That's what you lose, unfortunately. And they're great and people enjoy them more and more.

Yeah. So I agree. And for, I just remember, for 17 terabytes, it was 128 threads of all G or PGPCR, I don't remember. Wow. But with the focal discs S3, I need to update my knowledge.

#### Edge Case Problems

Technology is changing too fast. Certainly, hundreds of course. Terabytes of RAM already, right? But it does go straight to your point of the smaller they are, the faster you can recover still. And you don't hit some of these limits, like these systems were not designed with these types of limits in mind. Some weird data structure, you know, suddenly the limit of this is only, you know, hundred items, you know, and you hit those limits and then you are stuck.

Like recently, Metronome had an issue. Yeah, they had that, that outage, the multi-exact thing, which nobody has ever run before, but they hit that problem. Yeah, we have so many problems also when you only add, and yeah, it pushes forward positive, actually, sometimes. But if you want to be on the safe side, but I really like it, it's kind of resilience, characteristics, even if it's down, it's only a small part of your system is down. That's great.

Yeah, that's, that's mature architecture already. That actually makes it easier to achieve five nines of time, because that's the way you calculate, like if only one node is down, you divide it by the number of users.

#### Current Progress Update

Let's go. Cool. I think it's maybe one of the longest episodes we had. Enjoy that.

Oh, my God. I enjoyed it. I hope we will continue a discussion of issues with logical, for example, and so on. And maybe if things will be improved and so on. Looking forward to test POC once you have it. Thank you so much. Thank you.

It's any last things you wanted to add? Or anything you wanted to help from people on? I would say it feels like nothing is happening on the repository, except me pushing, you know, a few things changes. But a huge amount of work is happening in the background. Like some of these design work about consensus are all like almost ready to go. And there's also hiring going on. There are people coming on board very soon. So you will see this snowball. It's a very tiny snowball right now, but it's going to get very big as momentum builds up. So pretty excited about that. We may still have one or two spots open to add to the team, but it's filling up fast. So if any of you are very familiar, this is a very high bar to

#### Team Building Plans & Final Thoughts

You have to understand Consensus, Query Processing, but if there are people who want to contribute, we are still looking for maybe one or two people and also on the orchestration side and the Kubernetes side of things. I wish. Oh my God. I almost hope that day never comes, but it is so fun working on this project, creating it. Why do I want to give it to an AI to do it, you know?

Good, thank you, enjoy it a lot.

Yeah, thank you so much for joining us, it's great to have you as part of the Postgres community now and I'm excited to see what you get up to. And we too. Thank you.

Wonderful, thanks so much. Thank you. Bye bye.