use std::{collections::BTreeMap, fmt::Display, time::Duration};

use chrono::{DateTime, Utc};

pub use chrono;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
pub struct PingContractOptions {
    /// Time to live for the ping record.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "clap", clap(long, value_parser = duration_parser, default_value = "5s"))]
    pub ttl: Duration,

    /// The frequency to send ping record.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "clap", clap(long, value_parser = duration_parser, default_value = "1s"))]
    pub frequency: Duration,

    /// The tag of the ping contract subscriber.
    #[cfg_attr(feature = "clap", clap(long))]
    pub tag: String,

    /// Code hash of the ping contract.
    #[cfg_attr(feature = "clap", clap(long))]
    pub code_key: String,
}

#[cfg(feature = "clap")]
#[inline]
fn duration_parser(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

/// Maximum number of ping entries to keep per peer
const MAX_HISTORY_PER_PEER: usize = 10;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone)]
pub struct Ping {
    /// BTreeMap, not HashMap: contract state is compared byte-for-byte across
    /// peers to decide whether they have converged, and a HashMap serializes in
    /// iteration order, so two peers holding the same logical state encode it
    /// differently and never agree. Canonical encoding is a platform requirement
    /// (freenet-core #5320).
    from: BTreeMap<String, Vec<DateTime<Utc>>>,
    /// Optional padding to inflate serialized size for streaming tests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub padding: Option<Vec<u8>>,
}

impl core::ops::Deref for Ping {
    type Target = BTreeMap<String, Vec<DateTime<Utc>>>;

    fn deref(&self) -> &Self::Target {
        &self.from
    }
}

impl core::ops::DerefMut for Ping {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.from
    }
}

impl Ping {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a Ping with padding of the given size in bytes.
    pub fn with_padding(size: usize) -> Self {
        Self {
            from: BTreeMap::new(),
            padding: Some(vec![0xAB; size]),
        }
    }

    #[cfg(feature = "std")]
    pub fn insert(&mut self, name: String) {
        let now = Utc::now();
        self.from.entry(name.clone()).or_default().push(now);

        // Keep only the last MAX_HISTORY_PER_PEER entries
        if let Some(entries) = self.from.get_mut(&name)
            && entries.len() > MAX_HISTORY_PER_PEER
        {
            // Sort in descending order (newest first)
            entries.sort_by(|a, b| b.cmp(a));
            // Keep only the newest MAX_HISTORY_PER_PEER entries
            entries.truncate(MAX_HISTORY_PER_PEER);
        }
    }

    /// Merge another peer's ping state into this one.
    ///
    /// The retention rule is applied to the UNION of both sides, not to the
    /// incoming side alone. Filtering only `other` made the merge order-dependent:
    /// the same expired timestamp survived if this peer already held it and vanished
    /// if it arrived from a peer, so `merge(A, B)` and `merge(B, A)` disagreed
    /// permanently. That is a broken merge law (freenet-core #5320), and it was
    /// found by running the conformance verifier against this contract.
    ///
    /// The retention policy itself is unchanged and deliberate: keep the newest
    /// `MAX_HISTORY_PER_PEER` entries regardless of age, plus any older entries
    /// still within TTL.
    ///
    /// # TTL is measured against a LOGICAL clock, not the wall clock
    ///
    /// The reference instant is the newest timestamp anywhere in the merged state,
    /// not `Utc::now()`. This is the single most important thing to copy from this
    /// contract, so it is worth being explicit about why.
    ///
    /// The merge laws (freenet-core #5320) are statements about inputs determining
    /// outputs: `merge(A, B) == merge(B, A)`, `merge(A, A) == A`, and so on. A merge
    /// that reads the host clock is not a function of its inputs at all, so it
    /// cannot satisfy any of them except by luck. Concretely, with a wall clock two
    /// peers handed the SAME pair of states reach different states whenever a TTL
    /// boundary falls between the moments they each ran the merge. `fdev
    /// verify-merge` reports that as `update_determinism`, which is an ENFORCEABLE
    /// property — the tier that is eventually meant to justify removing a contract
    /// from the network.
    ///
    /// Be precise about how bad that is, because overstating it is its own kind of
    /// wrong. This particular divergence is TRANSIENT, not permanent. The wall-clock
    /// predicate is monotone in time — once an entry is old enough to be dropped it
    /// never becomes young enough to be kept — and it was already applied to the
    /// union rather than to the incoming side alone (#5352). Under those two
    /// conditions two replicas whose clocks differ by at most δ re-converge once
    /// wall time has carried both of them past the boundary. The old code was
    /// therefore convergent, and this change is not repairing a permanent split.
    ///
    /// What it repairs is that the merge was not a function of its inputs. That
    /// costs three things worth having: the reference contract stops producing
    /// removal-eligible evidence against itself, the divergence window (however
    /// short) disappears rather than being reasoned about, and — the part that
    /// matters most for a file people copy — the example stops demonstrating that
    /// reading the clock in a merge is acceptable if you argue carefully enough
    /// afterwards.
    ///
    /// Deriving the instant from the state itself makes the whole function pure.
    /// `max` over the union is exactly the right shape for this: it is idempotent,
    /// commutative and associative, so both merge orders compute the same reference
    /// and therefore prune identically, and it only ever moves forward as states
    /// merge.
    ///
    /// The two costs are real and worth understanding before copying this:
    ///
    /// 1. **A state nobody is writing to stops ageing.** With no new timestamps the
    ///    reference does not advance, so nothing further expires — and what is
    ///    already there stays. Be precise about that, because the obvious reassurance
    ///    is wrong: `MAX_HISTORY_PER_PEER` is a FLOOR, not a bound. `retain_history`
    ///    keeps the newest ten entries PLUS every older entry still within TTL of the
    ///    reference, so a peer that accumulated 500 timestamps inside one TTL window
    ///    keeps all 500 once writes stop (measured, and still 500 after five further
    ///    merges); under the old wall clock the same state decayed to ten with no
    ///    traffic at all. A live state settles at roughly `ttl × write_rate` per peer
    ///    and a frozen one holds whatever it settled at. Peer names are never swept
    ///    either. Nothing here is an absolute cap, so a contract that needs one has to
    ///    add it, and `validate_state` is the place. What the change does buy is that
    ///    the retained set is now a function of the state alone: it is arguably the
    ///    more honest behaviour, since with no new information a convergent type has
    ///    no basis for a new decision.
    /// 2. **A future-dated timestamp drags the reference forward** and expires
    ///    everything older than `future - ttl` at once. The per-peer floor does hold —
    ///    each peer keeps its newest ten — but that floor is the whole of the
    ///    protection, and ping deliberately does not guard the rest:
    ///
    ///    - It is **global, not local to the sender.** The reference is a max over the
    ///      whole union while the cap is per-peer, so one entry filed under one
    ///      unrelated name truncates EVERY peer's history to ten, discarding entries
    ///      that are well inside TTL. Measured: three peers holding 30 entries each
    ///      drop to 10 apiece after a single injected entry dated a year ahead.
    ///    - It is **permanent.** That entry is the newest under its own name, so it is
    ///      always inside its own newest-ten and is never evicted; the reference stays
    ///      pinned a year ahead and TTL retention stays dead. Five subsequent
    ///      legitimate pings do not recover it.
    ///    - It is **unauthenticated.** `validate_state` in the contract crate
    ///      deserializes and returns `Valid` with no plausibility check of any kind,
    ///      so any participant can inject it in an ordinary UPDATE.
    ///
    ///    That is a genuine regression against the wall clock rather than a wash:
    ///    under the old rule a future timestamp had no cross-peer effect at all, and
    ///    any local oddity ended as wall time carried past it. A contract that cannot
    ///    accept the trade wants `validate_state` to reject implausible timestamps —
    ///    which is a fine place to read the clock, because rejecting an input is not a
    ///    merge. Ping does not, so anyone copying this pattern should decide that
    ///    deliberately rather than inherit the omission.
    ///
    /// Note where the clock legitimately IS read: [`Ping::insert`], which records a
    /// new observation. Reading the clock at the WRITE is what makes it data;
    /// reading it at the MERGE is what makes the merge non-deterministic. If you take
    /// one rule from this contract, take that one.
    pub fn merge(
        &mut self,
        mut other: Self,
        ttl: Duration,
    ) -> BTreeMap<String, Vec<DateTime<Utc>>> {
        // Preserve the larger padding. Rewriting this function wholesale dropped this
        // step, which is a convergence bug of exactly the kind this change exists to
        // fix: a peer starting with `padding: None` that merges an update carrying
        // `Some(..)` would never adopt it, so the two would disagree forever on a
        // field neither side is wrong about.
        // Longer wins; equal lengths are broken by content, NOT left-biased.
        //
        // The original compared lengths only, so two paddings of the same length but
        // different bytes kept whichever side happened to be `self`: merge(A, B) kept
        // A's bytes and merge(B, A) kept B's, and the two peers then disagreed
        // forever. That is the same defect this change exists to remove, on a
        // different field, and the doc comment above claimed commutativity while the
        // code broke it. Latent rather than live — the only constructor fills a
        // uniform byte — but the field is public and arbitrary bytes deserialize.
        //
        // Comparing content on a tie is a total order over the padding itself, so
        // both merge orders pick the same winner.
        let replace_padding = match (&self.padding, &other.padding) {
            (Some(existing), Some(incoming)) => match existing.len().cmp(&incoming.len()) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal => existing < incoming,
            },
            (None, Some(_)) => true,
            _ => false,
        };
        if replace_padding {
            self.padding = other.padding.take();
        }

        let before = self.from.clone();

        // Union first. Nothing is judged before both sides are in one place, which
        // is what keeps the rule symmetric.
        for (name, incoming) in other.from {
            self.from.entry(name).or_default().extend(incoming);
        }

        // The logical clock: the newest timestamp anywhere in the union. Computed
        // AFTER the union so both merge orders see the same value — `max` over a set
        // does not care how the set was assembled. See the doc comment above for why
        // this is not `Utc::now()`.
        //
        // `None` means the union holds no timestamps at all, in which case there is
        // nothing to prune and the empty-entry sweep below does the remaining work.
        if let Some(reference) = self.from.values().flatten().max().copied() {
            for timestamps in self.from.values_mut() {
                Self::retain_history(timestamps, reference, ttl);
            }
        }

        // Remove empty entries. `or_default()` above creates one for any name the
        // incoming state mentions, including with an empty list, and nothing else
        // would ever remove it: a hand-built payload naming a peer with no timestamps
        // would otherwise persist forever and propagate to every peer that merged
        // from this one. It also keeps `len()` (map keys) in step with
        // `contains_key()` (non-empty).
        self.from.retain(|_, timestamps| !timestamps.is_empty());

        // Report peers that gained a timestamp we did not have.
        //
        // Deliberately NOT "anything that differs from before": `retain_history`
        // sorts and prunes, so a plain diff also fires when entries were merely
        // reordered or aged out, and the one caller
        // (`ping_client.rs::record_received`) counts each reported peer as a ping
        // RECEIVED. That would inflate the stats, including reporting a ping from
        // this node itself the first time its own unsorted entries get sorted.
        //
        // Note this is not quite what the original did, and is deliberately better:
        // the original compared list LENGTH after truncation, so once a peer was at
        // MAX_HISTORY_PER_PEER a genuinely new timestamp that displaced an older one
        // left the length unchanged and was never counted — a chronic under-count in
        // exactly the steady state where every peer is at the cap.
        let mut updates = BTreeMap::new();
        for (name, timestamps) in &self.from {
            let had = before.get(name);
            let gained = match had {
                Some(previous) => timestamps.iter().any(|t| !previous.contains(t)),
                None => true,
            };
            if gained {
                updates.insert(name.clone(), timestamps.clone());
            }
        }
        updates
    }

    /// Newest first, deduplicated, keeping `MAX_HISTORY_PER_PEER` regardless of age
    /// plus any older entries still within TTL.
    ///
    /// Sorting is by timestamp descending, a total order over the entries' own
    /// content, so the surviving set does not depend on the order they arrived in.
    ///
    /// `reference` is the instant TTL is measured back from. It is supplied by the
    /// caller rather than read here, and [`Ping::merge`] supplies the newest
    /// timestamp in the merged state — never the wall clock. See that function's
    /// documentation for why.
    ///
    /// # The TTL branch does less work here than it looks like it does
    ///
    /// Note it is only reached when a peer's entries EXCEED `MAX_HISTORY_PER_PEER`.
    /// Below the cap nothing is ever expired, however old it is, and `Ping::insert`
    /// already truncates to the cap locally — so the branch is entered only in the
    /// transient where two nodes hold different windows of the same peer's history
    /// and their union overflows. The cap does nearly all the bounding; the TTL
    /// trims the overlap.
    ///
    /// Worth being honest about, because it means this contract demonstrates the
    /// DISCIPLINE of expiry-inside-a-convergent-merge well and the NEED for it
    /// poorly. Anyone reaching for it as the reference for "how do I do TTL"
    /// should know that the size bound here comes from the cap, and design their
    /// own accordingly rather than assuming a TTL is load-bearing because this one
    /// is present.
    ///
    /// # Nothing here is a tombstone
    ///
    /// Every entry is a positive fact ("this peer was seen at t"). Dropping one is
    /// forgetting a positive fact, and a peer that still holds it may re-send it,
    /// at which point it is simply re-evaluated against the same rule — harmless.
    ///
    /// Expiring a TOMBSTONE — a recorded negative fact, "this was deleted" — is a
    /// different and genuinely unsafe shape, at any clock skew: forget the removal
    /// and the removed thing resurrects from any replica that still holds it, then
    /// propagates. If you add deletion to a contract shaped like this one, the
    /// deletion marker cannot be expired on the same terms as the data.
    fn retain_history(
        timestamps: &mut Vec<DateTime<Utc>>,
        reference: DateTime<Utc>,
        ttl: Duration,
    ) {
        timestamps.sort_by(|a, b| b.cmp(a));
        timestamps.dedup();

        if timestamps.len() > MAX_HISTORY_PER_PEER {
            let mut keep = timestamps[..MAX_HISTORY_PER_PEER].to_vec();
            keep.extend(
                timestamps[MAX_HISTORY_PER_PEER..]
                    .iter()
                    .filter(|t| reference <= **t + ttl)
                    .copied(),
            );
            *timestamps = keep;
        }
    }

    /// Gets the last timestamp for a peer, if available
    pub fn last_timestamp(&self, name: &str) -> Option<&DateTime<Utc>> {
        self.from
            .get(name)
            .and_then(|timestamps| timestamps.first())
    }

    /// Checks if a peer has any ping entries
    pub fn contains_key(&self, name: &str) -> bool {
        self.from.get(name).is_some_and(|v| !v.is_empty())
    }

    /// Returns the number of peers with ping entries
    pub fn len(&self) -> usize {
        self.from.len()
    }

    /// Returns whether there are no ping entries
    pub fn is_empty(&self) -> bool {
        self.from.is_empty()
    }
}

impl Display for Ping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries: Vec<_> = self.from.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        write!(
            f,
            "Ping {{ {} }}",
            entries
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: [{}]",
                        k,
                        v.iter()
                            .map(|dt| dt.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    /// Padding must survive a merge.
    ///
    /// Rewriting `merge` wholesale dropped this, and the omission is a convergence
    /// bug of exactly the kind this contract is being fixed for: a peer holding
    /// `None` that merges an update carrying `Some(..)` would never adopt it, so two
    /// peers disagree forever on a field neither is wrong about. Caught in review,
    /// pinned here.
    #[test]
    fn merge_adopts_padding_from_the_other_side() {
        let mut empty = Ping::new();
        assert!(empty.padding.is_none());

        let padded = Ping::with_padding(64);
        empty.merge(padded, Duration::from_secs(30));
        assert_eq!(
            empty.padding.as_ref().map(Vec::len),
            Some(64),
            "a peer with no padding must adopt the other side's"
        );

        // And the larger of the two wins, rather than the most recent.
        let mut small = Ping::with_padding(8);
        small.merge(Ping::with_padding(128), Duration::from_secs(30));
        assert_eq!(small.padding.as_ref().map(Vec::len), Some(128));

        let mut large = Ping::with_padding(128);
        large.merge(Ping::with_padding(8), Duration::from_secs(30));
        assert_eq!(
            large.padding.as_ref().map(Vec::len),
            Some(128),
            "merging a smaller padding must not shrink ours, or the two orders \
             disagree"
        );
    }

    /// Equal-length paddings must resolve the same way whichever side merges.
    ///
    /// The length-only comparison kept whichever side happened to be `self`, so two
    /// peers holding same-length different-content padding disagreed forever — the
    /// defect this contract is being fixed for, on a different field, while the doc
    /// comment claimed commutativity. Found by review; the state map's own
    /// commutativity test cannot catch it, because `Ping` has no `PartialEq` and the
    /// comparison goes through `Deref` to the timestamp map only.
    #[test]
    fn equal_length_padding_resolves_commutatively() {
        let ttl = Duration::from_secs(30);
        let mut a = Ping::new();
        a.padding = Some(vec![0x01; 16]);
        let mut b = Ping::new();
        b.padding = Some(vec![0x02; 16]);

        let mut a_then_b = a.clone();
        a_then_b.merge(b.clone(), ttl);
        let mut b_then_a = b.clone();
        b_then_a.merge(a.clone(), ttl);

        assert_eq!(
            a_then_b.padding, b_then_a.padding,
            "same-length paddings must resolve identically in both merge orders, or \
             the two peers never agree on the serialized state"
        );
    }

    /// A new timestamp that displaces an older one at capacity is still a ping.
    ///
    /// The original compared list length after truncation, so at
    /// MAX_HISTORY_PER_PEER a genuinely new timestamp left the length unchanged and
    /// went uncounted — an under-count in exactly the steady state where every peer
    /// sits at the cap. Pinned because it is the one place the reporting rule
    /// deliberately differs from what it replaced.
    #[test]
    fn updates_reports_a_new_timestamp_that_displaces_one_at_capacity() {
        // The held entries are all EXPIRED, so they survive only as the
        // newest-MAX-regardless-of-age set. A fresh arrival then pushes the oldest
        // past the cap, where being expired means it is dropped — a genuine
        // displacement with the length unchanged. (With everything inside TTL the
        // list would simply grow past the cap instead, since the rule keeps older
        // entries that are still fresh, and nothing would be displaced at all.)
        let ttl = Duration::from_secs(5);
        let expired = Utc::now() - Duration::from_secs(600);

        let mut ping = Ping::new();
        let full: Vec<_> = (0..MAX_HISTORY_PER_PEER)
            .map(|i| expired + Duration::from_secs(i as u64))
            .collect();
        ping.from.insert("Peer".to_string(), full);

        let mut other = Ping::new();
        other.from.insert("Peer".to_string(), vec![Utc::now()]);

        let updates = ping.merge(other, ttl);
        assert!(
            updates.contains_key("Peer"),
            "a displacing timestamp is a ping received, even though the list length \
             did not change"
        );
        assert_eq!(ping.from["Peer"].len(), MAX_HISTORY_PER_PEER);
    }

    /// Merging a state with itself changes nothing.
    ///
    /// Idempotence is one of the three laws this change is about, and nothing pinned
    /// it directly.
    #[test]
    fn merge_is_idempotent() {
        let ttl = Duration::from_secs(600);
        let mut ping = Ping::new();
        ping.insert("Alice".to_string());
        ping.insert("Bob".to_string());
        ping.padding = Some(vec![0xAB; 8]);
        // One merge first, so the state is already in the canonical form a peer would
        // actually hold; idempotence on unsorted input would be a weaker claim.
        ping.merge(Ping::new(), ttl);

        let before = ping.clone();
        ping.merge(before.clone(), ttl);

        assert_eq!(
            *ping, *before,
            "merging a state with itself must not change it"
        );
        assert_eq!(ping.padding, before.padding, "nor its padding");
    }

    /// A peer named with no timestamps must not become a permanent phantom entry.
    ///
    /// The union step creates an entry for every name the incoming state mentions,
    /// so a payload naming a peer with an empty list would otherwise persist forever
    /// and propagate to everyone who merged from this peer. It also keeps `len()`
    /// (map keys) in step with `contains_key()` (non-empty).
    #[test]
    fn merge_does_not_leave_empty_entries_behind() {
        let mut ping = Ping::new();
        ping.insert("Alice".to_string());

        let mut phantom = Ping::new();
        phantom.from.insert("Nobody".to_string(), Vec::new());

        ping.merge(phantom, Duration::from_secs(30));

        assert!(
            !ping.contains_key("Nobody"),
            "an empty entry must not survive the merge"
        );
        assert_eq!(
            ping.len(),
            1,
            "len() counts map keys, so a phantom entry would desync it from \
             contains_key()"
        );
    }

    /// `updates` reports peers that gained a timestamp, not merely changed bytes.
    ///
    /// The caller counts each reported peer as a ping RECEIVED, so reporting a peer
    /// whose entries were only reordered or pruned inflates the statistics — and the
    /// first merge sorts this node's own unsorted entries, which would report a ping
    /// received from itself.
    #[test]
    fn updates_reports_only_peers_that_gained_entries() {
        let mut ping = Ping::new();
        // Two entries inserted in ascending order, which `insert` does not sort while
        // under the history limit; the first merge will sort them descending.
        let older = Utc::now() - Duration::from_secs(2);
        let newer = Utc::now();
        ping.from.insert("Self".to_string(), vec![older, newer]);

        // Merging an empty ping changes the byte layout (sorting) but adds nothing.
        let updates = ping.merge(Ping::new(), Duration::from_secs(30));
        assert!(
            !updates.contains_key("Self"),
            "sorting our own entries is not a ping received from ourselves"
        );

        // A genuinely new timestamp is reported.
        let mut other = Ping::new();
        other.from.insert(
            "Self".to_string(),
            vec![Utc::now() + Duration::from_secs(1)],
        );
        let updates = ping.merge(other, Duration::from_secs(30));
        assert!(
            updates.contains_key("Self"),
            "a new timestamp must still be reported"
        );
    }

    /// Expired entries are retained on BOTH sides, or on neither.
    ///
    /// Updated for the #5320 conformance fix. This previously asserted that an
    /// incoming peer whose only entry was expired got dropped, while
    /// `test_keep_newest_entries_regardless_of_ttl` and
    /// `test_preserve_max_history_when_all_expired` assert that this peer's OWN
    /// expired entries are kept. Those two expectations are the same rule applied
    /// asymmetrically, and the asymmetry is what broke commutativity: the same
    /// expired timestamp survived if you held it and vanished if a peer sent it, so
    /// `merge(A, B)` and `merge(B, A)` disagreed permanently.
    ///
    /// The retention policy is unchanged — newest `MAX_HISTORY_PER_PEER` regardless
    /// of age — and is now applied to the union, so an incoming expired entry is
    /// kept exactly as an own expired entry is.
    #[test]
    fn test_merge_expired_is_symmetric() {
        let old_time = Utc::now() - Duration::from_secs(6);
        let ttl = Duration::from_secs(5);

        // Build the two inputs ONCE and clone them per direction. Constructing them
        // separately stamps `Utc::now()` again, so the two orders would be fed
        // different inputs and the comparison would say nothing about the merge.
        let mut a = Ping::new();
        a.insert("Alice".to_string());
        a.insert("Bob".to_string());
        let mut b = Ping::new();
        b.from.insert("Alice".to_string(), vec![old_time]);
        b.from.insert("Charlie".to_string(), vec![old_time]);

        let mut a_then_b = a.clone();
        a_then_b.merge(b.clone(), ttl);

        // Charlie is kept, on the same rule that keeps our own expired entries.
        assert!(a_then_b.contains_key("Alice"));
        assert!(a_then_b.contains_key("Bob"));
        assert!(
            a_then_b.contains_key("Charlie"),
            "an incoming expired entry must be retained on the same terms as an own \
             expired entry, or the merge is order-dependent"
        );

        // The property that matters: merging the other way round agrees.
        let mut b_then_a = b.clone();
        b_then_a.merge(a.clone(), ttl);
        assert_eq!(
            *a_then_b, *b_then_a,
            "merge must be commutative; this is the defect the conformance verifier \
             found against the deployed ping contract"
        );
    }

    /// Build a pair whose union exceeds `MAX_HISTORY_PER_PEER`, so that the TTL
    /// branch of `retain_history` is actually reached.
    ///
    /// Below the cap nothing is ever pruned, which makes it very easy to write a
    /// TTL test that exercises no TTL at all. Every test in this pair goes through
    /// here so that the branch under test is definitely live.
    fn split_across_two_states(name: &str, timestamps: &[DateTime<Utc>]) -> (Ping, Ping) {
        assert!(
            timestamps.len() > MAX_HISTORY_PER_PEER,
            "the union must exceed the cap or retain_history never reaches its TTL \
             branch and the test proves nothing"
        );
        let mut a = Ping::new();
        let mut b = Ping::new();
        for (i, t) in timestamps.iter().enumerate() {
            let side = if i % 2 == 0 { &mut a } else { &mut b };
            side.from.entry(name.to_string()).or_default().push(*t);
        }
        (a, b)
    }

    /// TTL is measured back from the state's own newest timestamp, not from the
    /// moment the merge runs.
    ///
    /// This is the test that fails if the logical clock is reverted to `Utc::now()`.
    /// Every timestamp here is an hour old, so a wall clock finds the entire tail
    /// expired and returns exactly `MAX_HISTORY_PER_PEER` entries; the logical clock
    /// puts the TTL window's edge 60s before the newest entry in the state and keeps
    /// the one tail entry that falls inside it.
    ///
    /// An hour-old state is not a contrived case — it is any contract that was busy
    /// and then went quiet, which is most of them.
    #[test]
    fn merge_prunes_against_the_states_own_newest_timestamp_not_the_wall_clock() {
        let ttl = Duration::from_secs(60);
        let newest = Utc::now() - Duration::from_secs(3600);

        // Ten entries at the cap, then two older ones: one inside the TTL window
        // measured from `newest`, one outside it.
        let mut timestamps: Vec<DateTime<Utc>> = (0..MAX_HISTORY_PER_PEER)
            .map(|i| newest - Duration::from_secs(i as u64))
            .collect();
        let inside_ttl = newest - Duration::from_secs(30);
        let outside_ttl = newest - Duration::from_secs(90);
        timestamps.push(inside_ttl);
        timestamps.push(outside_ttl);

        let (mut a, b) = split_across_two_states("Alice", &timestamps);
        a.merge(b, ttl);

        let kept = &a.from["Alice"];
        assert!(
            kept.contains(&inside_ttl),
            "an entry 30s older than the state's newest observation is within a 60s \
             TTL and must be kept; a wall-clock merge drops it purely because the \
             state as a whole is old, which is what makes such a merge disagree with \
             the same merge run a moment later"
        );
        assert!(
            !kept.contains(&outside_ttl),
            "an entry 90s older than the newest observation is outside a 60s TTL and \
             must still be pruned — expiry is not being disabled, only anchored"
        );
        assert_eq!(
            kept.len(),
            MAX_HISTORY_PER_PEER + 1,
            "the newest MAX_HISTORY_PER_PEER plus the one in-window tail entry"
        );
    }

    /// Merging the same pair twice, at two different moments, must give the same
    /// answer — which is only another way of saying `merge` is a function of its
    /// inputs.
    ///
    /// The tail entry is positioned so that a TTL boundary is crossed DURING this
    /// test: it is 200ms short of expiring when the first merge runs and 200ms past
    /// expiring when the second does. A merge reading `Utc::now()` therefore returns
    /// 11 entries and then 10. The logical clock returns the same answer both times
    /// no matter how long the gap is, so the shipped code cannot make this flaky —
    /// the timing only matters to the reverted version this is meant to catch.
    ///
    /// This is the shape of the real production divergence: not one merge going
    /// wrong, but two peers running the same merge either side of a boundary and
    /// reaching different states. That divergence is transient — the wall-clock
    /// predicate is monotone in time, so both sides drop the entry once wall time
    /// carries them past it — which is exactly why this needs a test rather than
    /// being noticed in the field.
    #[test]
    fn merging_the_same_pair_at_two_moments_gives_the_same_answer() {
        let ttl = Duration::from_secs(1);
        let now = Utc::now();

        let mut timestamps: Vec<DateTime<Utc>> = (0..MAX_HISTORY_PER_PEER)
            .map(|i| now - Duration::from_millis(i as u64))
            .collect();
        // Crosses the wall-clock TTL boundary 200ms from now.
        timestamps.push(now - ttl + Duration::from_millis(200));
        // Never in the window, on either clock: the pruning still has work to do.
        timestamps.push(now - Duration::from_secs(30));

        let (a, b) = split_across_two_states("Alice", &timestamps);

        let mut first = a.clone();
        first.merge(b.clone(), ttl);

        std::thread::sleep(std::time::Duration::from_millis(400));

        let mut second = a.clone();
        second.merge(b.clone(), ttl);

        assert_eq!(
            *first, *second,
            "merge must be a pure function of its inputs; a merge that reads the \
             host clock returns a different answer either side of a TTL boundary, \
             and two peers that merge the same pair at different moments then \
             disagree forever"
        );
        assert_eq!(
            first["Alice"].len(),
            MAX_HISTORY_PER_PEER + 1,
            "the in-window tail entry is kept and the 30s-old one is not, so the \
             boundary case is genuinely exercised rather than passing vacuously"
        );
    }

    /// Commutativity across the expiry boundary, which is where a merge that prunes
    /// is most likely to lose it.
    ///
    /// Both orders are given the identical pair, so any disagreement is the merge's
    /// own. The final assertion checks the boundary was actually straddled: without
    /// it the test would still pass if TTL pruning were deleted outright, which is
    /// the failure mode a commutativity test most easily hides.
    #[test]
    fn merge_is_commutative_across_the_expiry_boundary() {
        let ttl = Duration::from_secs(60);
        let newest = Utc::now() - Duration::from_secs(600);

        let mut timestamps: Vec<DateTime<Utc>> = (0..MAX_HISTORY_PER_PEER)
            .map(|i| newest - Duration::from_secs(i as u64))
            .collect();
        // Three tail entries stepping across the 60s window edge.
        timestamps.push(newest - Duration::from_secs(45));
        timestamps.push(newest - Duration::from_secs(59));
        timestamps.push(newest - Duration::from_secs(61));

        let (a, b) = split_across_two_states("Alice", &timestamps);

        let mut a_then_b = a.clone();
        a_then_b.merge(b.clone(), ttl);
        let mut b_then_a = b.clone();
        b_then_a.merge(a.clone(), ttl);

        assert_eq!(
            *a_then_b, *b_then_a,
            "merge(A, B) must equal merge(B, A) when the union straddles the expiry \
             boundary"
        );
        assert_eq!(
            a_then_b["Alice"].len(),
            MAX_HISTORY_PER_PEER + 2,
            "two of the three tail entries are inside the window and one is not, so \
             pruning genuinely ran; equal-but-unpruned would satisfy commutativity \
             while saying nothing about it"
        );
    }

    #[test]
    fn test_merge_ok() {
        let mut ping = Ping::new();
        ping.insert("Alice".to_string());
        ping.insert("Bob".to_string());

        let mut other = Ping::new();
        let recent_time = Utc::now() - Duration::from_secs(4);
        other.from.insert("Alice".to_string(), vec![recent_time]);
        other.from.insert("Charlie".to_string(), vec![recent_time]);

        ping.merge(other, Duration::from_secs(5));

        assert_eq!(ping.len(), 3);
        assert!(ping.contains_key("Alice"));
        assert!(ping.contains_key("Bob"));
        assert!(ping.contains_key("Charlie"));
    }

    #[test]
    fn test_history_limit() {
        let mut ping = Ping::new();
        let name = "Alice".to_string();

        // Insert more than MAX_HISTORY_PER_PEER entries
        for _ in 0..MAX_HISTORY_PER_PEER + 5 {
            ping.insert(name.clone());
            // Add a small delay to ensure different timestamps
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Verify we only kept the maximum number of entries
        assert_eq!(ping.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);

        // Verify they're sorted newest first
        let timestamps = ping.from.get(&name).unwrap();
        for i in 0..timestamps.len() - 1 {
            assert!(timestamps[i] > timestamps[i + 1]);
        }
    }

    #[test]
    fn test_merge_preserves_history() {
        let mut ping1 = Ping::new();
        let mut ping2 = Ping::new();
        let name = "Alice".to_string();

        // Insert 5 entries in ping1
        for _ in 0..5 {
            ping1.insert(name.clone());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Insert 5 different entries in ping2
        for _ in 0..5 {
            ping2.insert(name.clone());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Merge ping2 into ping1
        ping1.merge(ping2, Duration::from_secs(30));

        // Should have 10 entries for Alice now
        assert_eq!(ping1.from.get(&name).unwrap().len(), 10);

        // Verify they're sorted newest first
        let timestamps = ping1.from.get(&name).unwrap();
        for i in 0..timestamps.len() - 1 {
            assert!(timestamps[i] > timestamps[i + 1]);
        }
    }

    #[test]
    fn test_preserve_max_history_when_all_expired() {
        // Create a ping with expired entries
        let mut ping = Ping::new();
        let name = "Alice".to_string();

        // Insert MAX_HISTORY_PER_PEER entries, all expired
        let expired_time = Utc::now() - Duration::from_secs(10);
        for i in 0..MAX_HISTORY_PER_PEER {
            let timestamp = expired_time - Duration::from_secs(i as u64); // Make different timestamps
            ping.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Ensure entries are sorted newest first
        ping.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a short TTL so all entries would normally be expired
        let ttl = Duration::from_secs(5);

        // Create an empty ping to merge with
        let other = Ping::default();

        // Merge - this should preserve all entries despite being expired
        ping.merge(other, ttl);

        // Verify all entries are still there
        assert_eq!(ping.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);
    }

    #[test]
    fn test_remove_only_expired_entries_beyond_max() {
        let mut ping = Ping::new();
        let name = "Alice".to_string();
        let now = Utc::now();

        // Insert 5 fresh entries
        for i in 0..5 {
            ping.from
                .entry(name.clone())
                .or_default()
                .push(now - Duration::from_secs(i));
        }

        // Insert 10 expired entries
        let expired_time = now - Duration::from_secs(20); // well beyond TTL
        for i in 0..10 {
            ping.from
                .entry(name.clone())
                .or_default()
                .push(expired_time - Duration::from_secs(i));
        }

        // Sort entries (newest first)
        ping.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a TTL of 10 seconds
        let ttl = Duration::from_secs(10);

        // Create an empty ping to merge with
        let other = Ping::default();

        // Merge - should keep all fresh entries and enough expired ones to reach MAX_HISTORY_PER_PEER
        ping.merge(other, ttl);

        // Verify we have MAX_HISTORY_PER_PEER entries
        assert_eq!(ping.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);

        // Verify the first 5 entries are the fresh ones
        let entries = ping.from.get(&name).unwrap();
        for entry in entries.iter().take(5) {
            assert!(now - entry < chrono::TimeDelta::seconds(10)); // These should be fresh
        }
    }

    #[test]
    fn test_keep_newest_entries_regardless_of_ttl() {
        let mut ping1 = Ping::new();
        let mut ping2 = Ping::new();
        let name = "Alice".to_string();
        let now = Utc::now();

        // Add 5 fresh entries to ping1
        for i in 0..5 {
            let timestamp = now - Duration::from_secs(i);
            ping1.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Add 5 expired entries to ping2, but newer than ping1's entries
        // These should be kept despite being expired because they're the newest
        let expired_but_newer = now + Duration::from_secs(10); // in the future (newer)
        for i in 0..5 {
            let timestamp = expired_but_newer - Duration::from_secs(i);
            ping2.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Sort both sets
        ping1.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));
        ping2.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a very short TTL so basically everything is expired except the very newest
        let ttl = Duration::from_secs(1);

        // Merge ping2 into ping1
        ping1.merge(ping2, ttl);

        // Verify the result has MAX_HISTORY_PER_PEER entries
        assert_eq!(ping1.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);

        // The first 5 entries should be the ones from ping2 (they're newer)
        let entries = ping1.from.get(&name).unwrap();
        for entry in entries.iter().take(5) {
            assert!(*entry > now); // These should be the future timestamps
        }
    }

    #[test]
    fn test_consistent_history_after_multiple_merges() {
        let mut ping_main = Ping::new();
        let name = "Alice".to_string();
        let now = Utc::now();

        // Create several pings with different timestamps, ensuring they are clearly distinct
        let mut ping1 = Ping::new();
        let mut ping2 = Ping::new();
        let mut ping3 = Ping::new();

        // Use more explicit timestamps to avoid any potential overlap issues
        let timestamps_ping1: Vec<DateTime<Utc>> = (0..4)
            .map(|i| now - Duration::from_secs(30 + i * 2))
            .collect();
        let timestamps_ping2: Vec<DateTime<Utc>> = (0..4)
            .map(|i| now - Duration::from_secs(20 + i * 2))
            .collect();
        let timestamps_ping3: Vec<DateTime<Utc>> = (0..4)
            .map(|i| now - Duration::from_secs(10 + i * 2))
            .collect();

        // Add entries to each ping
        for timestamp in &timestamps_ping1 {
            ping1.from.entry(name.clone()).or_default().push(*timestamp);
        }
        for timestamp in &timestamps_ping2 {
            ping2.from.entry(name.clone()).or_default().push(*timestamp);
        }
        for timestamp in &timestamps_ping3 {
            ping3.from.entry(name.clone()).or_default().push(*timestamp);
        }

        // Sort all sets
        ping1.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));
        ping2.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));
        ping3.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a TTL that would expire some but not all entries
        let ttl = Duration::from_secs(25);

        // Merge in random order to test consistency
        ping_main.merge(ping2, ttl); // Middle
        ping_main.merge(ping1, ttl); // Oldest
        ping_main.merge(ping3, ttl); // Newest

        // Define the time range boundaries for classifying entries
        let ping3_min = now - Duration::from_secs(18);
        let ping2_min = now - Duration::from_secs(28);

        // Get the final entries
        let entries = ping_main.from.get(&name).unwrap();

        // The retention rule is "the newest MAX_HISTORY_PER_PEER regardless of age,
        // PLUS any older entry still within TTL", so the result is a lower bound of
        // MAX_HISTORY_PER_PEER, not an upper one. This assertion used to read
        // `<= MAX_HISTORY_PER_PEER` and passed only because TTL was measured from
        // `Utc::now()`: the newest entry here is 10s old, so a wall clock put the
        // 25s window's far edge at t-25s and expired the two oldest entries, while
        // the logical clock puts it at t-35s (25s before the state's own newest
        // observation) and keeps one of them. That is the intended consequence of
        // measuring TTL against the state rather than against the moment the merge
        // happens to run, so the assertion is corrected to state the actual rule
        // rather than the accident.
        assert!(
            entries.len() >= MAX_HISTORY_PER_PEER,
            "the newest MAX_HISTORY_PER_PEER are kept regardless of age, so the \
             result can never be shorter than that; got {}",
            entries.len()
        );
        let newest = entries[0];
        for extra in entries.iter().skip(MAX_HISTORY_PER_PEER) {
            assert!(
                newest <= *extra + ttl,
                "an entry beyond the newest MAX_HISTORY_PER_PEER may only survive \
                 if it is still within TTL of the state's newest observation"
            );
        }

        // The entries should be sorted newest first
        for i in 0..entries.len() - 1 {
            assert!(
                entries[i] > entries[i + 1],
                "Entries not correctly sorted at positions {} and {}",
                i,
                i + 1
            );
        }

        // Verify the newest entries are from ping3
        assert!(
            entries[0] >= now - Duration::from_secs(18),
            "Expected newest entry to be from ping3"
        );

        // Count entries by source time range
        let mut ping3_count = 0;
        let mut ping2_count = 0;
        let mut ping1_count = 0;

        for entry in entries {
            if *entry >= ping3_min {
                ping3_count += 1;
            } else if *entry >= ping2_min {
                ping2_count += 1;
            } else {
                ping1_count += 1;
            }
        }

        // Since TTL is 25s, all ping3 entries (4) and most ping2 entries should be included
        assert_eq!(
            ping3_count, 4,
            "Expected all 4 entries from ping3 (newest), but found {ping3_count}"
        );

        // Check that we have at least 3 entries from ping2
        assert!(
            ping2_count >= 3,
            "Expected at least 3 entries from ping2 (middle), but found {ping2_count}"
        );

        // Due to TTL, we expect at most 3 entries from ping1
        assert!(
            ping1_count <= 3,
            "Expected at most 3 entries from ping1 (oldest), but got {ping1_count}"
        );

        // Verify total count matches what we found
        let total_classified = ping3_count + ping2_count + ping1_count;
        assert_eq!(entries.len(), total_classified, "Entry count mismatch");
    }

    #[test]
    fn test_empty_after_merge_if_all_expired() {
        let mut ping = Ping::new();
        let name = "Alice".to_string();

        // Add some entries but all expired
        let expired_time = Utc::now() - Duration::from_secs(20);
        for i in 0..MAX_HISTORY_PER_PEER - 1 {
            // Less than MAX_HISTORY_PER_PEER entries
            let timestamp = expired_time - Duration::from_secs(i as u64);
            ping.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Sort entries
        ping.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a TTL shorter than the age of entries
        let ttl = Duration::from_secs(10);

        // Create an empty ping to merge with
        let other = Ping::default();

        // This should keep all entries despite being expired since we have less than MAX_HISTORY_PER_PEER
        ping.merge(other, ttl);

        // Verify all entries are kept
        assert_eq!(
            ping.from.get(&name).unwrap().len(),
            MAX_HISTORY_PER_PEER - 1
        );
    }
}
