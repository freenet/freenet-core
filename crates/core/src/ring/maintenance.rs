use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::{Duration, Instant}};
use either::Either;
use rand::seq::SliceRandom;
use crate::{message::{NodeEvent, Transaction}, node::{EventLoopNotificationsSender, PeerId}, operations::connect, router::Router};
use super::{LiveTransactionTracker, Location, Ring};

impl Ring {
}
