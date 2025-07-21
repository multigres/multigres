---
slug: database-school
title: 'Interview: Multigres on Database School'
authors: [sugu]
date: 2025-07-01
---

# Interview: Multigres on Database School

Sugu discusses Multigres on the Database School YouTube channel. He shares the history of Vitess, its evolution, and the journey to creating Multigres for Postgres. The conversation covers the challenges faced at YouTube, the design decisions made in Vitess, and the vision for Multigres.

<!--truncate-->

<iframe
  width="700"
  height="450"
  src="https://www.youtube-nocookie.com/embed/28q9mFh87KY"
  title="YouTube video"
  frameBorder="0"
  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
  allowFullScreen
></iframe>

## Chapters

- [00:00](https://www.youtube.com/watch?v=28q9mFh87KY) - Intro
- [1:38](https://www.youtube.com/watch?v=28q9mFh87KY&t=98s) - The birth of Vitess at YouTube
- [3:19](https://www.youtube.com/watch?v=28q9mFh87KY&t=199s) - The spreadsheet that started it all
- [6:17](https://www.youtube.com/watch?v=28q9mFh87KY&t=377s) - Intelligent query parsing and connection pooling
- [9:46](https://www.youtube.com/watch?v=28q9mFh87KY&t=586s) - Preventing outages with query limits
- [13:42](https://www.youtube.com/watch?v=28q9mFh87KY&t=822s) - Growing Vitess beyond a connection pooler
- [16:01](https://www.youtube.com/watch?v=28q9mFh87KY&t=961s) - Choosing Go for Vitess
- [20:00](https://www.youtube.com/watch?v=28q9mFh87KY&t=1200s) - The life of a query in Vitess
- [23:12](https://www.youtube.com/watch?v=28q9mFh87KY&t=1392s) - How sharding worked at YouTube
- [26:03](https://www.youtube.com/watch?v=28q9mFh87KY&t=1563s) - Hiding the keyspace ID from applications
- [33:02](https://www.youtube.com/watch?v=28q9mFh87KY&t=1982s) - How Vitess evolved to hide complexity
- [36:05](https://www.youtube.com/watch?v=28q9mFh87KY&t=2165s) - Founding PlanetScale & maintaining Vitess solo
- [39:22](https://www.youtube.com/watch?v=28q9mFh87KY&t=2362s) - Sabbatical, rediscovering empathy, and volunteering
- [42:08](https://www.youtube.com/watch?v=28q9mFh87KY&t=2528s) - The itch to bring Vitess to Postgres
- [44:50](https://www.youtube.com/watch?v=28q9mFh87KY&t=2690s) - Why Multigres focuses on compatibility and usability
- [49:00](https://www.youtube.com/watch?v=28q9mFh87KY&t=2940s) - The Postgres codebase vs. MySQL codebase
- [52:06](https://www.youtube.com/watch?v=28q9mFh87KY&t=3126s) - Joining Supabase & building the Multigres team
- [54:20](https://www.youtube.com/watch?v=28q9mFh87KY&t=3260s) - Starting Multigres from scratch with lessons from Vitess
- [57:02](https://www.youtube.com/watch?v=28q9mFh87KY&t=3422s) - MVP goals for Multigres
- [1:01:02](https://www.youtube.com/watch?v=28q9mFh87KY&t=3662s) - Integration with Supabase & database branching
- [1:05:21](https://www.youtube.com/watch?v=28q9mFh87KY&t=3921s) - Sugu's dream for Multigres
- [1:09:05](https://www.youtube.com/watch?v=28q9mFh87KY&t=4145s) - Small teams, hiring, and open positions
- [1:11:07](https://www.youtube.com/watch?v=28q9mFh87KY&t=4267s) - Community response to Multigres announcement
- [1:12:31](https://www.youtube.com/watch?v=28q9mFh87KY&t=4351s) - Where to find Sugu

## Transcription

{/*
docker pull ghcr.io/shun-liang/yt2doc

docker run ghcr.io/shun-liang/yt2doc --video "https://www.youtube.com/watch?v=28q9mFh87KY"
*/}

#### Intro

Welcome back to Database School, I am your host, Aaron Francis. In this episode, I talk to the co-creator of Vitess and the co-founder of PlanetScale. The same person. His name is Sugu Sougoumarane. We talk about his time at YouTube and the invention of Vitess, moving on to PayPal, then founding PlanetScale. And finally, his time in the wilderness. He took a little sabbatical and then he comes back to create Vitess for Postgres. He's joining Supabase to bring Vitess to Postgres. You're really going to enjoy this - he's incredibly smart but also very, very humble and very thoughtful. So, enough yapping from me, let's enjoy this episode of Database School.

This is kind of some breaking news.

So, I'm super excited to be here. I am here with Sugu Sougoumarane. He is the co-creator of Vitess, the co-founder of PlanetScale. And he is back to talk with us about his next adventure. But before we get into that, we're going to dive into a little bit about the history of Vitess. And then we'll talk about what's up next. So, Sugu, thank you for being here. Do you want to introduce yourself a little bit? Sure, yeah. I'm Sugu. And I have been involved with databases for a pretty long time, I think since the 90s. Wow. I was working at Informix and then I moved on to PayPal in the early days and took care of scalability there. And from there, I moved

#### The birth of Vitess at YouTube

to YouTube, and that is where I co-created Vitess with my colleague Mike Solomon. This was because YouTube was falling apart on the database side, and we had to come up with something that would leap us ahead in terms of the troubles it was going through. We can talk about that more later, but eventually Vitess, we decided to donate it to CNCF, and by that time Vitess had a lot of adoption - Slack started using it, Uber was using it, and more users were coming on board. At that time CashApp was also using it, so it was time to start a company fully dedicated to backing this software, which is how I ended up co-founding PlanetScale with Jiten, who was also with me at YouTube. He was actually on the SRE side, taking care of Vitess. And about three years ago, by the time it had been 12 years since I was working on Vitess, and three years ago, I had successfully built a full team that was taking care of the project, and I said, okay, it is time to take a break, so I went on sabbatical. And recently this itch came back that I need to do something, and when I saw Postgres, I just couldn't control myself, and so here I am. I love it.

#### The spreadsheet that started it all

That's a lot, and we're going to cover it all. Let's start, take me back to the YouTube days. So you're at YouTube, and YouTube is taking off, and y'all just can't keep up. Everything's falling apart all over the place. And so, what was the conversation like, when you or you and your co-creator went to somebody and said, what if we invented something entirely new and wrote it from scratch? How did that come to be? The solution that ended up working? So that's actually a very good story. It was actually my co-creator who did this, who thought of this idea, who said that this seems like a losing war, because every day was actually worse than the previous day. And every time there was an outage, it was the database. So what he did was he went to a Starbucks - it wasn't a Starbucks, it's actually a coffee shop called Dana Street in Mountain View - and wrote a huge spreadsheet. I still actually have this spreadsheet, where he listed all the problems that we were facing, and also all the problems we are going to face as this thing grows, and listed a bunch of solutions, saying that, okay, how do we solve this problem, how do we solve this problem, and then you take a step back and look at everything, we knew what we had to build. And the answer at that time was, we needed a proxy layer that stands between the application and MySQL and protects it. That was kind of the high level statement, and that is kind of how Vitess was born, and we basically said that in order to build this, we need a very different focus from where we are today, which is, our focus was always like, how do we fight the next fire, or how do we survive till next Tuesday? So those were the kind of thoughts that were in our mind. We actually pulled ourselves out of the day-to-day operations and said, we are going to take ourselves out, you guys are on your own, and we'll build this and come back with something that will solve all our problems. It was very ambitious.

Yeah, but it worked, so you and your co-creator set up a special unit, and you said, we're not involved with firefighting, we're not involved with fixing problems, we're going to do this big bet, and this big bet is, we're going to basically rewrite, not really, we're going to write a proxy layer that pretends to be MySQL and sits between the application and the actual databases. And we will do a bunch of stuff in there, so tell the people, what exactly, just kind of at a high level, what is Vitess, and why did...

#### Intelligent query parsing and connection pooling

What is it about Vitess that solves all of those problems?

So, what Vitess is today is nothing like what we initially conceived. It evolved kind of iteratively over time. The initial conception of Vitess was just cluster management and protection of the database. And the MySQL database at that time had connection problems, which is actually still a problem with Postgres now, but now there are solutions coming up, but that was a problem with MySQL also. So, the first product that we built was just a connection pooler. Get traffic and pool it across just a few connections. And that took a while actually because we didn't build it as a simple connection pooler. We kind of knew that a dumb connection pooler will not take us far. We actually built an intelligent connection pooler that actually understood queries. We actually parsed every query because we knew that we had to filter queries, judge them and say that this is a bad query. We did not let it go to the database. Those were kind of the thoughts that we had because the bad queries would be the ones that could actually take our databases down all the time. So, that's the reason why we went one level above the immediate requirement. And it took us about a year finally to get it launched. And at that time, it was not Vitess, but what happened was once that server was out there in the middle, people realized, oh my God, there are things we can do here before we reach the database. So, feature requests started coming in and slowly, and because we had the parser, we could do some very intelligent things with our queries. And so, that is kind of what led to the large number of features that we could add to Vitess - without the parser, we wouldn't have been able to add them. And that eventually evolved into what Vitess is today, essentially emulating an entire database.

Yeah, so that's an interesting, that was an interesting and insightful call there at the beginning to say, instead of just doing dumb connection pooling, we're going to add a little bit of intelligence here to like inspect the queries. And it sounds like that was the thing, that was like the golden insight there. And then once you have that server in the middle, you now have, it's like, you now have a new bucket in your brain where you can put things. And you're like, oh, because we have this server in the middle, what if we did this and what if we did that, whereas when that doesn't exist, your brain doesn't even think like, well, we can't do that because we're just talking to the database. And I'm not going to go rewrite my sequel, but now that you have this thing, you can cast all of your hopes and dreams upon it. So what were some of the first things that came in, you're in this, you're in this, you know, secluded team writing this thing and you put it out to the world and everybody's like, great, you solved some of our problems, now can you do this, what were some of the first few big ones that came in.

#### Preventing outages with query limits

So, the first one is adding a limit clause to every query that the applications sent. Interesting, because in OLTP, you are not expected to fetch 100,000 rows to serve your web page. Right. So, what we did was, if your query had no limit clause, we would put one, and 10,000 was the limit. If the number of rows exceeded that limit, we would return an error. So, that was actually one of the biggest problems at YouTube, because people would assume how many videos can somebody have, and you would have a hypothetical answer. So, you would say, oh, just fetch all the videos, but there are people who have a huge number of videos, for example. Sure, yeah. And so, when this happened, this protected against a whole bunch of outages.

Another really, really cool feature that we added in the beginning was, this happened actually when, again, it's related to the number of videos. The YouTube home page, we used to highlight certain users, and show their videos on the home page, on YouTube's home page. So, one of the users that got highlighted had 250,000 videos, and that query had no limit clause. So, which means that every page hit would go and fetch those 250,000 videos. So, when you put this limit on there, and stuff just starts hitting a hard wall, it's just like, sorry, that doesn't work.

What do the application developers think at that point? Are they super happy that you've surfaced some sort of error, or where's the, as far as the internal teams go, you put this blocker on the database, and then what happens on the application developer side? Not all of them were happy.

Yeah. I think we had some level of arrogance to tell them to live with it.

Yeah. I'm not going to change this. And I guess in our case, it came from the fact that protecting the database is more important than keeping one developer happy. So, yes, I think we might have pissed off a few developers. So, are you on, at that point, is it just the ops team that you're on? So, at that point, it's ops versus, you know, application developers?

Yeah, I was. Yeah. We were kind of the, at that time, we were the architecture team. So, we also kind of dictated how applications were written, what rules to follow to access the database, we were deploying software. So, we were kind of - later, we became the ops team, but at that time, we were the architecture team.

Yeah. You're talking 2007? I know. That's why I was thinking probably DevOps probably wasn't around. No, DevOps didn't exist.

Yeah. So, you're just ops or architecture. Man, that's so, that's so interesting.

Okay. So, the first one was taking this, you know, query that shouldn't ever exist and could result in degraded performance or just knocking the whole thing over and stopping it at the border and saying, sorry, you can't come in here, error, go fix your stuff. That seems pretty reasonable. You said at the beginning, um...

#### Growing Vitess beyond a connection pooler

Vitess was fundamentally smaller and way different than what it's grown into now, So how did you guide that growing from an Intelligent connection pooler to what it is now, which is this massive sprawling thing?

Talk us through that journey of I assume a few years, I know that the whole thing has been running for several, but I have to imagine that first few years was pretty active. These are good questions, because nobody has asked these questions. So when we first deployed Vitess, it was just VT tablet by the way, and even VT tablet faced a lot of obstacles, because it was an additional layer and it added latency.

2011 is when we first launched, we started working on Vitess in 2010. In 2011, Go 1.0 was not even out yet. I think Go 1.0 came out in 2012 or something, so we were launching on a non-released version of Go and it was not efficient. I think at that time the latency was like 10 milliseconds, just horrendous. Now it's sub millisecond, and we were on hard disks by the way, there were no SSDs in those days. So pause there for one second. What was the choice to go with Go with it being so early? So it must have had some massive upsides that papered over all of these other problems. So what was the decision there? You wouldn't believe it - it was like a very quick decision. I hated Java and my co-creator hated C. Great, I love it. He said not Python, obviously because this needs to perform. How about this new language called Go? It's incredible. I love how human it all is. You all picked Go because it was the mutually agreed upon non-hated language. It's still pre-1.0.

#### Choosing Go for Vitess

Okay, so carry on, you're working with Go, it's got a 10 millisecond latency, and you're starting to grow. Yeah, and fortunately, I would say when we later learned about Go, we really, really liked the Go team's approach to the language. They were exactly thinking the way we were approaching problems. Like, you know, there is this fascination with complexity - we were like, things should be as simple as you can, you know, you should not overcomplicate. So they were like - we were pretty much in sync with them. So we really liked them. And since they were also at Google, they got wind that we were developing this. So they helped us quite a bit. They actually prioritized our problems and worked very hard at making things work for us, which is pretty awesome. So, yeah, so in VTTablet, we were basically adding more features to VTTablet. And by that time YouTube wasn't a happy place. But then came the first resharding experiment, because we had, I think, I don't remember anymore, I think we had 8 shards and we were going to 16. And so quaint, so long ago, that's crazy. And that was quite a nightmare. And then we said, oh, we need to add resharding, make it more automatic. And so that's when we introduced this whole metadata layer, where we actually, at the time, it was ZooKeeper. There was no etcd. So ZooKeeper, we used to store metadata in there and then use that to manage resharding. And that required the application to connect to ZooKeeper to figure out what to connect to, which made the application more complex. So every time you resharded, you had to basically push things to ZooKeeper and the application to reload that information saying that, you know, this is a new sharding configuration, you have to go there. And that's when we decided to introduce the VTGate layer. Because we can't be having changes in the resharding to affect the application. So we'll have this VTGate layer, which will watch over this topology. And you just send traffic to VTGate and VTGate will know which shard to send it to. Perfect. And you can start. Yeah. No, go ahead. It was not a database connection. It was actually a specialized RPC connection. Because the application still had to know which shard, which at the time we had this concept of a keyspace ID, which mapped to a shard. And the keyspace ID is now called - it's in Vitess. So previously, the application would say, send this query to that shard. And we changed the API to say, send this query to this keyspace ID. And VTGate would take the keyspace ID, look at the topology and say, oh, that maps to this shard. I'm sending it there. So if you resharded, then the application is unaffected. So that's what happened.

Okay. So you started with the VTTablet and then as a function of the painful reshard, you introduced the VTGate. So for the people listening, at this point in the history, talk us through from the application to the actual data, where are we connecting and who's talking to whom, where does VTGate, VTTablet and the running MySQL instance fit in from the application all the way through?

#### The life of a query in Vitess

Let's tell you the life of a query. I don't know if you've heard of the life of stories. That's it. That's perfect.

So, the application uses gRPC. Actually, in Google it is Stubby, right?

Stubby, yes. It's a...

You tell me, I don't know. It's Google's version of gRPC.

Okay. gRPC was born off that project. I should actually confirm the project. I think it is Stubby. Anyway, it connects via gRPC to one of the VTGates. Then sends a query, you know, SELECT *. But in the query, it says, this query belongs to this keyspace ID, which means that it's a SELECT from a user. It also says, the user lives at this keyspace ID. And so then VTGate says, Oh, okay, let me go look up this keyspace ID in my topology. And the topology says,

Oh, this keyspace ID is in this key range. And that maps to this shard. Okay. And so, okay, then VTGate says,

Okay, which VTTablets are present that serve this shard? That is also in the topology. And it gets a list. And it chooses one randomly for a read query. It's a random choice.

And then it sends the query to VTTablet saying,

Please serve this query for me. And VTTablet has all the protection there. If there's a limit clause missing, it'll add it. If it's an invalid query, it'll reject it.

And then VTTablet sends the query to MySQL, which serves your query. And the results are returned back to the application - the life of a query. I love it. That's very easy. That's very straightforward. So is it fair to say that at this point in the history of Vitess, the VTGate just handles the pointing to the correct VTTablet. And so who or rather how is the application developer signaling to VTGate the what did you call it keyspace ID that is not mapped to shard, but is loosely mapped to shard by somebody else, how's the application developer saying this user Aaron Francis is in keyspace ID, one, two, three, like where does that come from? So that knowledge was already present when we sharded - when we did the initial shard. Got it.

Okay. And there were two methods. One is the first sharding technique was hashing of the user ID. Actually, the first sharding technique was actually not a hash of user ID - it was actually a random assignment of a user ID to a shard. And we had a table that was a lookup table that said where that user lived. We moved away from that and then changed it to a hash of user ID. So user ID would get hashed to

#### How sharding worked at YouTube

a keyspace ID, and then that keyspace ID is mapped to a range-based shard that was split in our case equally split, like 8, 16, 32, 64, that way. That was for user IDs, but if you had a SELECT by video ID, we wanted to keep our video IDs with our users. For video IDs, you had to know which user ID the video ID belonged to, and we held it in a table. You were smiling, because you know where this is going. And that video ID, so we would look up the table, find out the user ID for the video ID, and then do a hash, and send the query to a shard that had that user.

Okay. And so where do we live now? Like, you know, this is all groundbreaking at the time, but I'm sure looking back on it, you're like, oh my goodness, what in the world?

So where does it live now - all of the pieces of Vitess, and who is responsible for what?

Because from an outsider's perspective of somebody just like, we'll say connecting to a PlanetScale database seems pretty easy to me. I don't really have to do anything. And so some of those components have gotten a lot smarter, and taken a lot of that off of the application developer. So what is the state of it right now, or when it became mature, maybe even not right now, but when it became mature, how do those components fit in with each other? So the biggest leap that we made, which transitioned Vitess from where it was then to where it is today, was a decision at that time that was really, really scary. This keyspace ID that I talked about was physically present as a column on every table. And if you wanted to know what is the keyspace ID of a row, you could select that row, and there's a column that said, this is your keyspace ID. The question that I asked myself is, can we live without that column? And can we live with the fact of hiding this from the application - what if the application didn't know the keyspace ID? Would the system continue to work as efficiently as it was before? And to push that question...

#### Hiding the keyspace ID from applications

question further, which means that if the application - or the converse of that question is, can you compute the keyspace ID using the WHERE clause of your SELECT statement? So, it took a few months of pure thinking to come to the conclusion that yes, this can work. So, before we get into how it could work, what made you ask that question? Because I was not happy that at that time, you could not write an ODBC driver that could connect to Vitess. So, you were looking at, I'm casting this upon you, so if this is wrong, tell me, you were looking at adoption and you were saying, hey, if this thing is going to get adoption, we got to play the game and we can't have custom connectors for everything. So, I'm surprised at how mature we were - we actually at that time felt that for Vitess to have a real future, even within YouTube, it needs to be adopted outside YouTube. That is very mature. That's an interesting insight. We felt that if YouTube was the only user of Vitess, eventually somebody is going to say, why maintain something this bespoke? So, that is what motivated me to think that we need a generic database API for Vitess.

So, you asked the hard question and then you spent many months thinking about it and then what happened? That was actually one of the hardest problems for me to solve. Essentially, it didn't stop there. What I had to do is go back and learn relational algebra. And figure out and prove to myself that sharding and combining those shards into a single unit can be modeled using traditional relational algebra. And that's why that was difficult. And then eventually mapping the relational algebra back to SQL.

I actually, the document should still be there in Vitess when I first wrote the initial document. And then I wrote an enormous query and showed how this - with all possible constructs of SQL - and showed how this maps to relational operations. And how such a relational operation would work in a sharded database. This is fascinating.

Okay, so let me get my thoughts here. So, you decided we are going to parse the query and then we are going to figure out what shard this needs to go to. So we are going to become basically like you're going to eat all of that pain on behalf of the application developers. And that's what that's what takes it from Vitess is pretty useful but pretty specialized to Vitess is extremely useful. And the application needs to know even less and Vitess will just do a lot more work. And so did you push the same query parser up to VT gate and use that or did you write?

Okay, so you use that same one.

Okay, so the parser is the same and then you're going to have to keep it high level. How did you determine from the query which shard it should go to? Just you're going to have to keep it up here for me but tell me just how did you do that?

Yes, so that was actually what I implemented is not what is there today. But at that time, so the way I started was let's take the simple case. If it is a select statement where your where clause is the shard in key, it is very straightforward. We like that.

Yeah, so you start with that and then you go to a slightly more complex construct which is a join. And the join is on the shard in key and the where clause is also the shard in key. Then the query still goes to the same shard that was like the next level of complexity. The third level of complexity is you select a query where you do the join on the shard in key. But there is no where plots. So if that happens, then you can actually you don't have to break you don't have to break that query up into smaller parts. You send that query to all shards because without changing it because all rows for each shard live within that row. And the fourth level of complexity is when you realize, oh, the rows for this shard are not in the same place. In which case, this is where relational, the relational algebra came into play where can you identify parts within this complex query where you can say this portion, this part of this query can still be preserved and sent to a single shard.

#### How Vitess evolved to hide complexity

So that was the aha moment for me, think that even if there is a complex query, you can identify parts that can be preserved as is and sent to individual shards. Incredible. So this is a, you know, I don't think learning about relational algebra and deconstructing queries is broadly applicable, but what you did is broadly applicable in that you started with can we make the simple case work, can we just make the easy one work?

Yes, we can. Let's move on to slightly harder, slightly harder, until you have kind of this framework built up in your head, to where you get to the hardest one, and you've kind of taken a few, yes, confidence building, but also like groundwork building steps to get there, such that you can tackle the hardest one there at the end. And is there a place still online where we can read this, you know, paper relational algebra post, whatever, is that still available somewhere?

I, I, I did a series video series. I lost my audience, I lost most of my audience, about three fourths of the way, still there on my website on YouTube channel, because it is, it is a very hard problem. Yeah, sounds like it. And unless you, you really, really, unless it's a matter of survival, you would not want to put yourself through that pain.

Okay. Well, I'm going to find that and I'll leave a link down below. So if any of you fellow database nerds want to watch it, I'll put a link down below. It was not planned. It is pure free form where I said, I'm just going to start talking a lot. I just kept talking, I, an hour past, I said, let's stop here. And then, and I think it's like six or seven parts. So like seven, eight hours of me just rambling. It's very boring. I would, I would not recommend that. That actually, I should talk about Andres who is at the planet scale. He is the, the first one who ingested that entire thing. Wow. And say, you know what, there's a better way to do this. Oh, cool. Oh, man, I love that. I am very pro reading original source material. And in this case, watching original source material. Because like you said, nobody does it. And if nobody is doing it, there's a huge amount of, there's a huge amount of an advantage that can be gained when you go straight to the source. So it sounds like that happened at planet scale, which brings us to the planet scale years, because I want to, I want to talk about.

#### Founding PlanetScale & maintaining Vitess solo

Vitess for Postgres, and spend a lot of our time there. So tell me just as briefly or as long as you want to, You founded PlanetScale with a co-founder and then you decided, I need some time off and you took three years off. So let's lump those two things together and tell me about that part of your story before you come back out of retirement for your triumphal return. So I was going to sabbatical retirement, I even called it retirement for a while.

When I co-founded PlanetScale with Jiten, I was the only one who left Google from the original Vitess team. So I was actually the sole maintainer of Vitess even by that time because YouTube was actually migrating to Spanner. So there was nothing to do with Vitess being bad. It is just a policy decision at Google that they wanted a uniform data store and they didn't want to be maintaining something that is bespoke. So at that time, I was the only one maintaining Vitess after that.

Vitess by the way is too complex to fit in one person's brain. That includes mine. And the way I managed it was by actually time compartmentalizing, which means that I would allocate a few months to just one area of Vitess and during that time, I was incapable of answering questions or helping anyone with anything else. I would focus on that and moved to something else, which is how I maintained it for about one and a half years or so. It was very stressful. But over this time, at PlanetScale, I managed to find great people like Vicken and Rishen, the entire Vitess team. And they ramped up very quickly. And there was a time when I was outproducing the entire team. And by 2022, I could barely keep up with a single one of them. They were all incredible. And that's when I realized, you know what, I've succeeded.

Yeah, no kidding. I hadn't taken a break. It was like 12 years or so. That's how I went on this sabbatical. Okay. And so you've built up, that's incredible, by the way. You've built up this foundational piece of software. And you get these great people in. And I, when I was at PlanetScale, worked with Deepthi. And I just love her. She's amazing. She's awesome. She's wonderful. I'm sure the rest of the team is great, but I worked with Deepthi. And she's a delight. So you built up this team of geniuses. And you stepped away from it, which was, I thought, going to be the hardest part. Well, she's doing it. She's doing a great job. So you built up this team of geniuses and you decided, all right, it's time to take a little rest. And then you go and do what for three years. Just chill out.

#### Sabbatical, rediscovering empathy, and volunteering

I would call it reinventing myself. I kind of look and behave like the same person. The way I would put it is I had a value system, which I think was good, I think people appreciated. But I don't think I was connected to it emotionally, within myself? It was more of just a behavior, and what I did in the last three years was basically worked on connecting myself to myself, if that makes sense. Fascinating. Can you give us any examples? Maybe one of the values that you felt like was just behavior, but you weren't connected to?

Empathy. When you see someone in pain, you empathize with them, but do you actually feel what they are feeling? Sometimes you say, oh, something bad is happening to them and then you go, you know you have to be nice to them, you have to say nice things, but do you actually feel what they are feeling? Those are the kinds of things that I worked on. Understanding, truly understanding people's pains, truly understanding people's joys, and actually relating to that is what I would... I actually, the transformation was so fundamental to me that I actually went into a lot of volunteer work because now I understand why you have to work with other humans, help others. So I for a while thought of doing that full time until I felt that I started missing tech work also, so I still do all the volunteer work, but now I am also doing the technical work. This is such a delightful story. So you go off into the wilderness, right? You go off into the wilderness, you reinvent yourself, you rediscover, you find yourself, and then you have the original call that's like pulling you back. So you've done the thing, you've gone off, and you've made yourself, you've become more yourself, and then the siren song of the thing that you walked away from starts calling back. So tell me...

#### The itch to bring Vitess to Postgres

What is it that started to bring you back, and why was it Postgres this time, instead of MySQL?

The idea of Postgres has always been sitting in the back of my mind, and it was constantly nagging me, and it's not new, it's been there since 2020. In 2020, there were a few people from the Postgres community that talked to me, and said, you know, we need to figure out a way to do this - there are actually even issues open in Vitess about it. They say, this problem needs solving, and I was excited, I wanted to do it, but then I feel very guilty, but I had to say, I can't focus on this. Sure, there was too much to take care of at PlanetScale. Too many things - we were still making Vitess work better, bringing up features, making the serverless application work. There was a lot of things going on, I was very sad to put an end to that project. I didn't put an end to it, I said, there will be another time, but not now. That has been in the back of my mind. Even during my sabbatical, I've talked to people saying that I reached out to an old Vitess contributor and told them, you start this Postgres thing for me. Nobody did, and so this has been slowly growing, and it became an obsession in the last couple of months when I realized Postgres is exploding. I am an advisor to a company called Metronome, and they had an outage, they published a postmortem, I saw that and I said, oh my god, this problem needs to be solved today. That's when I decided what can we do to restart this project. And I will say, I don't think you should feel guilty, but the thing that you told that person, I can't do it right now, but someday, is now true. Now is the day, you're doing it. So I don't know if that issue is still open, but you gotta go find it, and you say we're back, baby. So you've had this itch in the back of your head for a long time. Let's bring Vitess to Postgres. So talk to me about some of the things, as you're now focusing on that, so it's been something that's been there the whole time, but now you're like, let's do this. What are some of the things?

#### Why Multigres focuses on compatibility and usability

What are some things that you either want to do the same, or differently, from original, or current, Vitess, which we'll just call Vitess, and we're going to call the new one Multigres, is that right? Multigres, OK, so what are some things you want to do with Multigres that are either the same, or different than Vitess, things that worked great, or things that are like, man, now that I have a clean slate, let's redo it, and to color that answer, what are some of the things about Postgres that require you to do something slightly different, so you can take that anywhere you want, but I want to hear about your vision for Multigres. So let me talk about the things I feel like we did not prioritize in Vitess. Yeah, and this is no indictment, by the way, Vitess is a fundamentally successful project that is powering huge applications, so, but you have a chance to start over, so go on. The first one, compatibility, I would focus on compatibility, make sure that to the extent possible, every Postgres construct works like before. So Multigres should feel and act like a Postgres, love that, that is the highest priority. So when we built Vitess, that was not our goal, mainly because we thought Vitess would become its own standard and database. And that is not true, people, till even today, the first question they ask is, how compatible is it with us? So that is one problem. The other one that we did not really pay attention to is approachability and usability. Vitess is a very, very complex piece of software, extremely flexible, like you could go to Vitess and ask, can you do this very specific thing? The answer is most likely yes, because all you have to do is take this piece, take that piece, put it together and write a script, and it will do this for you. So this command line option that you need to change, like literally if you do VT tablet dash edge, you will get like, I pages of command line options. And yes, and you can change one of those to make Vitess do what you like. But that actually has been a huge barrier for adoption, because it's daunting to think about bringing up a Vitess cluster. So that is the second problem that I would definitely address the numerous degrees. What would be a third one? Let me see if I can bring up my notes here. I had some notes. I would say the other problem would be something that we are still struggling with. Actually, we finally have, I shouldn't say we, it's the VTest thing that did this, which is too PC. So the VTess workload has always encouraged not lying on two phase commits, distributed transactions. And because we have always believed that two phase transactions are a slippery slope, once you let you do it, you may go, abuse it and actually end up in a state where you're not happy. So we highly discouraged it to the extent that we didn't even want to support it.

And finally, I think the Vitess team realized that, you know, we should support it and they've added support. And that's something that I would prioritize for in postgres because again, it goes back to compatibility. If you don't do PC, you're not going to have compatibility. So those are the three things I would prioritize moving.

#### The Postgres codebase vs. MySQL codebase

Everything else I'll bring mostly. Yeah, that's huge. That's a huge endorsement of Vitess.

And so, with the architecture of Postgres, Does that make those things harder, easier, or does it have no effect? Because MySQL and Postgres are fundamentally different beasts. And so, you're not going to be able to just grab it all and move it over. Does that make your life a lot easier, that you're now doing it for Postgres instead of MySQL?

Yes, I think there are a few reasons why it's easier. One is Postgres has a much cleaner 2PC API, very beautiful, I saw it.

Oh, my God, whereas MySQL is not this simple. So, that's number one.

Compatibility is actually going to be easier, mainly because of how approachable the Postgres codebase is and how much tribal knowledge exists in Postgres, about the Postgres engine itself. For example, if we wanted to implement stored procedures, we would basically have to do it right now in Vitess, we would have to do it from the ground up. But if we were doing it in Postgres, we can take the entire existing stored procedure implementation in Postgres and just transport it into Vitess. So compare the source code of Postgres and MySQL for me real quick.

What is - I've read horror stories on Hacker News about the source code for MySQL. What is the - and you said Postgres is so clean and understandable. What's going on there? What's the big difference?

I think the big difference is that there are not as many experts in MySQL about the code because they are - I don't even know, like for example, I wouldn't even know where to ask is there somebody who knows how this engine works. And whereas in Postgres, there are so many places to ask. And I would say it's the openness of the community. Yeah. Yeah, that makes sense. Yeah. And I've heard - yeah. No, please.

Yeah, like it feels like any question that you have, you can get it answered probably, and also the licensing because it's PostgreSQL license, I can freely copy it. If it's MySQL license, even if I see the code, I cannot copy it. Gotcha. Well, that makes a huge difference. I have heard amazing things about the, not the Postgres user community, I haven't heard bad things, but about the Postgres hackers. I think is what they call themselves - the Postgres hackers. I've heard amazing things about that community, the people that actually work down on the core and how

#### Joining Supabase & building the Multigres team

Everything is just done out in the open. You just put it on the mailing list and you get to talk to people and gather consensus. But while that may sound like a slow and arduous process, it's all happening out in the open. And there are people that you can talk to, and you can find them at conferences or email them or hop on a call with them, and you can get all of that knowledge downloaded into your head. So that makes a lot of sense to me. So that part is going to make your life a lot easier.

How are you, or you and the team, you should say that you have joined Supabase to further this effort.

So how are you and whatever team you're building around yourself inside of Supabase? How are you going to develop it, structure it, organize it, both like your little new community that is budding and the actual software project. Like, where are you, are you going to do VTGates and VTTablets - is all of that architecture coming over? So maybe start with the organization of the team and your decision to join Supabase and then we can move into the technical organization. Yeah, that's a great question. So the idea, my idea of joining Supabase is like, you know, I have done this before. I was able to hire and train people. So building a team is not a problem. And there are also old contributors, contributors that are out there that I should be able to reach and get them on board. So I have confidence in building the right team and with the right people, I know it won't be hard to bring them up to speed. So that part, I'm not worried at all. And is that your mandate inside Supabase? Let's go grab some people and form this Multigres team. Find the right talent, get them ramped up and do it. So that's the team strategy.

Okay. And the technical strategy, what I want Multigres to be is a

#### Starting Multigres from scratch with lessons from Vitess

Postgres native project. In other words, I should put a no MySQL sign.

What we don't want is as two reasons, like one is anything MySQL specific is we don't want to live with that for project that is meant to last for many years for Postgres. So we don't want to inherit anything that we did just because it's MySQL. So that's one thing.

And the other thing is Vitess is an old project. It's 15 years old. It has legacy features that are either not in use or that we are supporting only because we don't want to break somebody. There's no reason to bring those in.

Also into this new project.

So for these two reasons, what we've decided to do was we are going to start from scratch and we are going to import copy over what we think we want instead of taking this as is and retrofitting it to make it to work for Postgres. Cool. So you get to do the thing that every developer always wants to do and that is a rewrite. You get to start over. So you get to start over, but not only are you starting over, you're starting over with a solid foundation from which you can pull and you're starting over with a mandate from a successful company. So that you can actually go out and build the team and you can spend the time to do this right normally rewrites are happening, you know, as the planes on the way down, you're building the parachute, but you're not doing that here. You've got the time, you've got the bandwidth, you've got the space. And so where do you even begin? Like you know, in the beginning, you said we started, you know, the VT gate, we started or the tablet or whatever we start with select star where ID equals and it's like, okay, we can do that. So in this situation, walk me through like, what are you actually where you actually going to start what component or what like Postgres extension or where's where are you going to get your hooks in?

The first one is actually to build a VT tablet and a VT gate, okay, a very basic version of the two. And actually, our goal is to deploy throughout super base, like which means that once this is ready, this will be the super base load balancer, this will be your super base front end, unified. And the reason why we want to do this is because once you got your connection, that connection stays until

#### MVP goals for Multigres

So, your database goes to 500 terabyte, you don't have that's the good stuff.

Yeah, you never have to know that your things are now completely different underneath, and basically, you should not know that you went chartered, you should not know that all these transitions took place, it should be completely turned. So, for that reason, what we want is Vitess to manage your life from the beginning, building a weekend scale to millions, you just changed to building a weekend scale to billions. That's amazing, so you're going to start with the router, which is the VT gate, and that's the one that the outside world, the application developers will connect to, and it will hold that connection and serve as kind of like an opaque layer that's like, don't worry, what's going on behind the curtain. We got it, we're in control, you connect to us, we'll handle the rest, so that's the, are you going to call them MG gates for Multigres? I think it's going to remain VT gate, I looked at it, that is just too much VT. Yeah, I was going to say, that's going to not only be hard technically, but you are going to continue to say VT gate forever, I guarantee it. So, yeah. So, you're going to start with the VT gate, which is the router and the VT tablet, which sits next to, in this case, Postgres, and handles the, is it still handling the sort of like blocking of bad queries and stuff like that, this VT gate. Some of those actually, we may actually not port all those features, because many of the, like, that's actually a good question. We may not port it initially, because we don't see, we don't see any, we don't see many people using those features anymore. I think, I think when we were there, because we were pushing the databases to the limit, these, we were very sensitive to these changes. But once you are scaled out, I don't think it's as big a deal.

Okay, cool. It's my guess. So, that raises my next question, which is, which are, if you had to put like a punch list of these are the three things that we definitely have to have to hit 0.1 release or whatever. So, what do you have those in mind? And if not just off the top, okay, let's hear them. What are the big guys that you're trying to hit? The VT tablet and the proxy layer. What I want, where I want to be is being able to serve charted queries. Okay. Maybe not stored procedures. Maybe don't know, but at least queries that are for insert update, you know, delete, select those, those four queries in a fully sharded postgres.

Okay. So, that would be what I would call my MVP.

Okay, that's your north stars. You want to be able to serve sharded queries. Yes. Transparently. Right. With no knowledge from the application developer, which is what makes VTES work in my opinion. So, what about like, what about the next features? Are you thinking, does that include resharding? And how much of, I guess this is, this is an interesting situation you find yourself in because you're coming into a mature company that has a lot of features. And so, are you going to be trying to pull some of those features and put them into multi-graph?

#### Integration with Supabase & database branching

How does that interplay, kind of work within Supabase? Well, the advantage is Supabase treats Postgres as a black box. And we become that back black box.

I'm thinking about something like database branching. Does that not feel like it crosses over between what Supabase is already doing and where Multigres is going to come in or is that something you have thought about? Because I have to imagine there are some places where it's going to be kind of blurry as to who's responsible.

That's a good question. I actually don't know how database branching works in Supabase, but I would presume that at the end of the day, even if you branch to database, you are going to create another one. So I think it will still work fine by the fact that Multigres is a black box that masquerades as Postgres. You want to create a copy, you create a copy and this will create a copy for you. So you should not care that it is a sharded Postgres under the covers. The way I would approach it, but what you say has a point, like some of these abstractions, you know, are may not be as perfect. In which case, I am in Supabase, so I could go talk to that even.

Oh, I can't wait to have you back in a year to figure out how all this stuff finally plays out. This is so fascinating to me. So where are you? Where's the project right now?

Your announcement came maybe last week. I feel like it was pretty recent. Last week, the week before something like that, you are now inside of Supabase. Talk to us about the status of both the project and the team. I learned two days learning Mac because I had previously. I was previously a Linux user for I think I last used Mac like, oh, at least over 10 years ago. I'm just laughing because you're like, you're like one of the four most like computer science brains in the world. And you join this company and you're like, I don't know what is this thing. I don't know what this is.

Okay, so you got a little bit of a learning curve. You're on Mac. All right. That's a good start. I'm making while, you know, I can still type. So I am making I'm actually making a feature list. I'm making a project plan.

Okay, I'm making. I'm also talking about some making some core changes that the old witness has that the new witness, you know, need not have. Some cool extensions that we don't have the luxury to do with the old witness that we can do with them because we now have a clean slate. So I'm brainstorming with myself right now because there's nobody else. And so I'm doing that, which is pretty exciting.

Yeah, so you're getting a dream. You're getting a dream a little bit. Yeah, it sounds fun. Yeah, so in your in your wildest dreams. Let's not talk about reality in your wildest dreams. Where do you see multigress going? Either from like a adoption and impact standpoint or from a feature standpoint, where you're like, you know what? I don't even want to say this out loud, but wouldn't it be crazy if? So as you're in this dreaming phase and planning and trying to like map out what is going to be a huge contribution to the database ecosystem. What are these things that you're like, oh, that would be incredible. Do you have any?

#### Sugu's dream for Multigres

I think this dream may sound simple, but my dream is someone coming in, and being able to say this is just Postgres, except that it can scale massively. I will say that that is a simple dream from a consumer's perspective, but probably a big, hairy audacious goal from your perspective, right? It has to feel like a huge goal. It's almost unachievable, but can we do it? That's what makes it so fun. They should not feel like it's a different system. Because that hurt us so much in Vitess. Other than that, I want to see, I mean this is something that Vitess is totally capable of, I would like to see databases that run into the petabytes comfortably.

Okay, those are good goals. I like that. I especially like the focus on, and maybe this is your empathy showing, the focus on the compatibility. Because one of the frustrating things is connecting to a Vitess cluster or plant scale database and finding out, oh, this doesn't work. They can't do correlated subqueries or whatever weird things that we developers like to do. And they've been hammering pretty hard on compatibility, but to hear that as a founding principle for Multigress is, I think that seems directionally correct to me. For every Suga who wants to build out a brand new database, there are five million developers that just want to run a database. And they just want it to work. And so having compatibility be so high up on the list, seems like a wise decision from my point of view. But I know that that's going to be super hard from your point of view. And so that's what makes it a good goal. And that's what makes it exciting. You're going to have to sit around for three months and think about things again. Simple and hard.

Yes, exactly. And from from the perspective of building the team, is there anything? Is there anything you want to say to anyone that might be listening of people you're looking for or how's that process going? Or like where are you trying to pull these people? How big of a team are you going to start with? Tell me about your team building thoughts. I've always liked smaller teams. I would say something something in the range of five to ten people as is what I would say the sweet spot. I still feel proud that with this has never had a team size greater than 10, for example, even it's crazy. And we were able to compete with all other distributed systems with this small team. So so I believe in the power of efficacy of a single engineer being able to you know, I think a small team works so well together that take and outperform extremely large teams. So with this has proven that and that has been the case even in my previous lives, both at YouTube and PayPal. So I would say it like five.

#### Small teams, hiring, and open positions

I'd like 5 to 10 people would be the number of people I have in mind. And anyone, I would say anyone that has worked on Vitess, will obviously be welcome, and it's, I assume it's all going to be the same stack, it's all going to be Go, same as Vitess, because you're going to pull over a lot of code from Vitess.

As a matter of fact, 90% of Vitess is actually agnostic of MySQL. Most people don't know this, because that's how we built Vitess - we did not want to get locked down with MySQL, which was smart, until we were pressured to, which is why it was not compatible, because we did not want to do anything specific with MySQL. And then later we said, okay, you know, we'll start working on that.

So, are you actively hiring right now? Yes, we are actively hiring. There is a link in the blog post at Supabase. So, if you are interested, click on that link, and please apply. Wonderful.

Yeah, we have already a bunch of applicants, I bet, interviewing a few. So, yes, and at some point in time, we will actually say we have enough, because I don't want to build a big team - 5 to 10 is not very many. Well, I'll leave a link down below for anyone that has either worked with Vitess or is a super Go genius or anything like that. Apply - anybody with distributed systems or deep database knowledge. Perfect, yes, I'll leave a link below for all of that. So, speaking of your blog post, you kind of came out of nowhere. You've been out in the wilderness, and then you come back and you're like, by the way, I'm joining Supabase, and we're doing Multigres. What was the response like to that? I mean, I saw it, but tell me from your point of view, what was the public's reaction?

#### Community response to Multigres announcement

It was overwhelmingly positive. I did not expect what I saw. I kind of felt like I come from the MySQL world, I thought I may not be accepted by the Postgres community, so I feel very humble that the people are welcoming me so I will keep that in mind. So I didn't expect to be welcomed this well and I will respect that for sure.

I think that is a testament both to you and your proof of work and to the Postgres community, that they are so excited to have you become like a pretty fundamental part of it doing this massive project.

So I'm glad to hear, that's what I saw from the outside as everybody was like, aw, this totally rules and I'm glad to hear that that was felt by you as well because I think it is well deserved. So I don't want to take up too much more of your time but tell me, is there anything else about Multigres or things you're looking forward to or things that are exciting for you or anything else you want to leave the audience with? Any just kind of broad thoughts or

#### Where to find Sugu

I think you covered everything that I wanted to say, and I would say I would talk about, now if you are interested, yeah, definitely looking for talent,

Wonderful, at this point, my top, the top of my mind is that, If you want to, if you're listening and you want to work with a thoughtful genius, I'll leave some links down below to where you can find Sugu, I have to tell you, you should be incredibly proud of all the things that you have done. You're extremely thoughtful, but also clearly massively intelligent, and the stuff that you have done is very impressive, and so I hope that you feel that, I know that you worked on it for 12 years, and then had to be like, I got to take a step back, But I hope you're coming back with tons of energy, and I hope you feel like, man, this is going to be great, because I think it is, and I think you should be super proud of all the stuff that you've done.

Oh, thank you very much.

Yeah, and thanks for coming on here. I'll leave links down below for everything we've talked about so people can find out more, and tell us just as we go here, where can people find you online if they want to connect? You can find me on LinkedIn - Sugu, you can find me on x.com - S, Sugu, and where else do I go? I can't even remember.

Okay, Sugu. Perfect, perfect, so if you want to go see his Reddit comments over there, I'll leave the links to all of that down below. Thank you all for listening.

Yeah, I know. I realize that I should have been more active. I don't know, I think, you know, if you're going to go on sabbatical, logging off seems like the right thing to do. So we'll say that that was the right thing to do, but now you're back, so people can go on LinkedIn and Twitter or Reddit, I suppose, and find you if they want to hang out. So thank you, Sugu, for being here. This has been just a delight. This has been so much fun, I hope you enjoyed this. It's just as much fun for me too.

Oh good, well that's very nice. If y'all are listening on audio, there is a video on YouTube, and if you're listening on YouTube, there is an RSS audio only feed, I'll leave links to both in the show notes or the description. Until the next time, we'll see you later.
