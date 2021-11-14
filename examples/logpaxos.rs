//! This is an implementation of Log Paxos, an algorithm that ensures a cluster of
//! servers never disagrees on an append only log of values.
//! https://decentralizedthoughts.github.io/2021-09-30-distributed-consensus-made-simple-for-real-this-time/
//! 
//! # The Algorithm
//!
//! The LogPaxos algorithm is comprised of two phases.
//!
//! ## Phase 1
//! Leadership election
//!
//! ## Phase 2
//! Appending and retrieving log values
//! ## Leadership Terms
//!
//! A term is maintained until the leader times out (as observed by a peer), 
//! allowing that leader to propose a sequence of values while (1) avoiding contention
//! and (2) only paying the cost of a single message round trip for each proposal.
//!
//! * Define a total order on proposers using the Id of the actor
//! * Proposers send keep-alive messages to each other every Δ.
//! * Proposers do not attempt phase 1, if they believe a ‘higher’ numbered peer is still alive
//! * If we do not recieve a keep-alive within 2Δ then treat the peer as unavailable

use serde::{Deserialize, Serialize};
use stateright::{Expectation, Model, Checker};
use stateright::actor::{Actor, ActorModel, DuplicatingNetwork, Id, majority, model_peers, Out};
use stateright::semantics::LinearizabilityTester;
use stateright::util::{HashableHashMap, HashableHashSet};
use std::borrow::Cow;
use std::ops::Range;
use std::iter::Iterator;
use std::cmp::Ordering;
use std::time::{ Duration };
use stateright::semantics::append_only_log::Log;
use stateright::actor::append_only_log::{LogActor, LogMsg, LogMsg::*};

type Round = u32;
type Ballot = (Round, Id);
type Proposal = (RequestId, Id, Value);
type RequestId = u64;
type Value = char;

const HEARTBEAT_TIME : u64 = 2000;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[derive(Serialize, Deserialize)]
enum LogPaxosMsg {
    Prepare { ballot: Ballot },
    Prepared { ballot: Ballot, last_accepted: Option<(Ballot, Log<Value>)> },

    Accept { ballot: Ballot, log : Log<Value> },
    Accepted { ballot: Ballot },

    Decided { ballot: Ballot, log: Log<Value> },
    KeepAlive
}
use LogPaxosMsg::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LogPaxosState {
    id : Id,
    phase: LogPaxosPhase,
    current_peer_ids : HashableHashSet<Id>,
    last_peer_ids : HashableHashSet<Id>,

    // shared state
    ballot: Ballot,

    // leader state
    proposal: Option<Proposal>,
    prepares: HashableHashMap<Id, Option<(Ballot, Log<Value>)>>,
    accepts: HashableHashSet<Id>,

    // acceptor state
    accepted: Option<(Ballot, Log<Value>)>,
    is_decided: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum LogPaxosPhase {
    Follower,
    LeaderPhase1,
    LeaderPhase2
}

#[derive(Clone)]
struct LogPaxosActor { 
    peer_ids: Vec<Id>,
    heartbeat_interval : Range<Duration>
}

impl LogPaxosActor {
    fn new(peer_ids : Vec<Id>) -> LogPaxosActor {
        let mut sorted_peer_ids = peer_ids.clone();
        sorted_peer_ids.sort();
        LogPaxosActor {
            peer_ids : sorted_peer_ids,
            heartbeat_interval : Duration::from_millis(HEARTBEAT_TIME/2)..Duration::from_millis(HEARTBEAT_TIME)
        }
    }

    fn get_max_accepted(accepted : Vec<Option<(Ballot, Log<Value>)>>) -> Option<(Ballot, Log<Value>)> {
        let mut result : Option<(Ballot, Log<Value>)> = None;  
        for entry in accepted {
            if let Some((ballot, log)) = entry {
                match &result {
                    Some((curr_ballot, _curr_log)) if curr_ballot < &ballot => {
                        result = Some((ballot, log))
                    }
                    Some((curr_ballot, curr_log)) if curr_ballot == &ballot => {
                        // take the longest log
                        if curr_log.cmp(&log) == Ordering::Less {
                            result = Some((ballot, log))
                        }
                    }
                    None => {
                        result = Some((ballot, log))
                    }
                    _ => {}
                }
            }
        }
        result
    }
    
    fn is_valid_proposal(state: &mut Cow<LogPaxosState>, proposed_log : &Log<Value>) -> bool {
        match &state.accepted {
            Some((_ballot, log)) => proposed_log.extends(&log),
            None => true 
        }
    }

    fn is_leader(&self, state: &LogPaxosState) -> bool {
        for peer_id in self.peer_ids.iter() {
            // if this actor has the highest ID it is the leader
            if &state.id < peer_id {
                return true;
            }
            let alive_peer_ids : Vec<&Id> = state.current_peer_ids.union(&state.last_peer_ids).clone().collect();
            if alive_peer_ids.contains(&peer_id) {
                return false
            }
        }
        true
    }
}

impl Actor for LogPaxosActor {
    type Msg = LogMsg<RequestId, Value, LogPaxosMsg>;
    type State = LogPaxosState;

    fn on_start(&self, id: Id, out: &mut Out<Self>) -> Self::State {
        out.set_timer(self.heartbeat_interval.clone());
        LogPaxosState {
            id : id,
            phase: LogPaxosPhase::Follower,
            current_peer_ids : Default::default(),
            last_peer_ids : Default::default(),
            // shared state
            ballot: (0, Id::from(0)),

            // leader state
            proposal: None,
            prepares: Default::default(),
            accepts: Default::default(),

            // acceptor state
            accepted: None,
            is_decided: false,
        }
    }

    fn on_msg(&self, id: Id, state: &mut Cow<Self::State>, src: Id, msg: Self::Msg, out: &mut Out<Self>) {        
        match msg {
            LogMsg::Get(request_id) if state.phase == LogPaxosPhase::LeaderPhase2 && state.is_decided => {
                let (_b, log) = state.accepted.clone().expect("decided but lacks accepted state");
                out.send(src, LogMsg::GetOk(request_id, log));
            }
            LogMsg::Push(request_id, value) if state.phase == LogPaxosPhase::LeaderPhase2 && state.is_decided  => {
                let mut state = state.to_mut();
                state.is_decided = false;
                state.proposal = Some((request_id, src, value));
                state.accepts = Default::default();
                let extended_log = match &state.accepted {
                    Some((_b, log)) => {
                        log.clone().append(state.proposal.unwrap().2)
                    }
                    None => {
                        Log::new().append(state.proposal.unwrap().2)
                    }
                };
                // Simulate `Accept` self-send.                            
                state.accepted = Some((state.ballot, extended_log.clone()));
                // Simulate `Accepted` self-send.
                state.accepts.insert(id);

                out.broadcast(&self.peer_ids, &Internal(Accept { ballot: state.ballot, log: extended_log }) );                
            }
            Internal(Prepare { ballot }) if state.ballot < ballot => {
                let mut state = state.to_mut();
                state.ballot = ballot;
                // if state.phase != LogPaxosPhase::Follower {
                //     log::info!("Node {:?} converting to follower", state.id);
                // }
                state.phase = LogPaxosPhase::Follower;
                out.send(src, Internal(Prepared {
                    ballot,
                    last_accepted: state.accepted.clone(),
                }));
            }
            Internal(Prepare { ballot }) if state.ballot > ballot => {
                out.send(src, Internal(Prepared {
                    ballot: state.ballot,
                    last_accepted: state.accepted.clone(),
                }));
            }
            Internal(Prepared { ballot, last_accepted }) 
                if ballot == state.ballot && 
                    state.phase == LogPaxosPhase::LeaderPhase1 => {
                let mut state = state.to_mut();
                state.prepares.insert(src, last_accepted);
                if state.prepares.len() == majority(self.peer_ids.len() + 1) {
                    // Ensure we use the longest existing committed log
                    let longest_log = match Self::get_max_accepted(state.prepares.values().cloned().collect()) {
                        Some((_b, log)) => {
                            log.clone()
                        }
                        None => {
                            Log::new()
                        }
                    };
                    // log::info!("Node {:} Become Leader", id);
                    state.phase = LogPaxosPhase::LeaderPhase2;
                    // Simulate `Accept` self-send.                            
                    state.accepted = Some((ballot, longest_log.clone()));
                    // Simulate `Accepted` self-send.                   
                    state.accepts.insert(id);
                    // align all peers with the leader
                    out.broadcast(&self.peer_ids, &Internal(Accept { ballot, log: longest_log.clone() }) );
                }
            }
            Internal(Prepared { ballot,  last_accepted: _ }) if ballot > state.ballot => {
                let mut state = state.to_mut();
                state.ballot = ballot;
                state.phase = LogPaxosPhase::Follower
            }            
            Internal(Accept { ballot, log }) if state.ballot <= ballot && LogPaxosActor::is_valid_proposal(state, &log) && state.phase == LogPaxosPhase::Follower => {
                let mut state = state.to_mut();
                state.is_decided = false;
                state.ballot = ballot;
                state.accepted = Some((ballot, log));
                out.send(src, Internal(Accepted { ballot }));
            }
            Internal(Accepted { ballot }) if ballot == state.ballot => {
                let mut state = state.to_mut();
                state.accepts.insert(src);
                if state.accepts.len() == majority(self.peer_ids.len() + 1) {
                    state.is_decided = true;
                    let (ballot, log) = state.accepted.clone().unwrap();
                    out.broadcast(&self.peer_ids, &Internal(Decided{ ballot, log }));
                    // notify original requester proposal has been accepted if there was one
                    if let Some(proposal) = state.proposal {
                        let (request_id, requester_id, _) = proposal.clone();
                        state.proposal = None;
                        out.send(requester_id, LogMsg::PushOk(request_id));
                    }
                }
            }
            Internal(Decided{ ballot, log }) => {
                let mut state = state.to_mut();
                state.ballot = ballot;
                state.accepted = Some((ballot, log));
                state.is_decided = true;
            }
            Internal(KeepAlive) => {
                state.to_mut().current_peer_ids.insert(src);
            }            
            _ => {}
        }
    }

    fn on_timeout(&self, id: Id, state: &mut std::borrow::Cow<Self::State>, out: &mut Out<Self>) {
        out.broadcast(&self.peer_ids, &Internal(KeepAlive));    
        let mut state = state.to_mut();
        // if this node should be leader and we are not active as leader, start new phase 1
        if self.is_leader(state) && state.phase == LogPaxosPhase::Follower {
            //log::info!("Node {:} Starting Leader Preparation", id);
            state.phase = LogPaxosPhase::LeaderPhase1;
            state.prepares = Default::default();
            // Simulate `Prepare` self-send.
            state.ballot = (state.ballot.0 + 1, id);
            // Simulate `Prepared` self-send.
            state.prepares.insert(id, state.accepted.clone());
            out.broadcast(&self.peer_ids, &Internal(Prepare { ballot: state.ballot }));
        }            
        state.last_peer_ids = state.current_peer_ids.clone();
        state.current_peer_ids = Default::default();
        out.set_timer(self.heartbeat_interval.clone());
    }
}

#[derive(Clone)]
struct LogPaxosModelCfg {
    client_count: usize,
    server_count: usize,
}

impl LogPaxosModelCfg {
    fn into_model(self) ->
        ActorModel<
            LogActor<LogPaxosActor>,
            Self,
            LinearizabilityTester<Id, Log<Value>>>
    {
        ActorModel::new(
                self.clone(),
                LinearizabilityTester::new(Log::<Value>::new())
            )
            .actors((0..self.server_count)
                    .map(|i| LogActor::Server(LogPaxosActor::new(model_peers(i, self.server_count))))
                    )
            .actors((0..self.client_count)
                    .map(|_| LogActor::Client {
                        put_count: 1,
                        server_count: self.server_count,
                    }))
            .duplicating_network(DuplicatingNetwork::No)
            .property(Expectation::Always, "linearizable", |_, state| {
                state.history.serialized_history().is_some()
            })
            .property(Expectation::Sometimes, "value chosen", |_, state| {
                for env in &state.network {
                    if let LogMsg::GetOk(_req_id, value) = env.msg.clone() {
                        if value != Log::new() { return true; }
                    }
                }
                false
            })
            .record_msg_in(LogMsg::record_returns)
            .record_msg_out(LogMsg::record_invocations)
    }
}

fn main() -> Result<(), pico_args::Error> {
    use stateright::actor::spawn;
    use std::net::{SocketAddrV4, Ipv4Addr};

    env_logger::init_from_env(env_logger::Env::default()
        .default_filter_or("info")); // `RUST_LOG=${LEVEL}` env variable to override

    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("check") => {
            let client_count = args.opt_free_from_str()?
                .unwrap_or(2);
            println!("Model checking Log Paxos with {} clients.",
                     client_count);
            LogPaxosModelCfg {
                    client_count,
                    server_count: 3,
                }
                .into_model().checker().threads(num_cpus::get())
                .spawn_dfs().report(&mut std::io::stdout());
        }
        Some("explore") => {
            let client_count = args.opt_free_from_str()?
                .unwrap_or(2);
            let address = args.opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!(
                "Exploring state space for Log Paxos with {} clients on {}.",
                client_count, address);
            LogPaxosModelCfg {
                    client_count,
                    server_count: 3,
                }
                .into_model().checker().threads(num_cpus::get())
                .serve(address);
        }
        Some("spawn") => {
            let port = 3000;

            println!("  A set of servers that implement Log Paxos.");
            println!("  You can monitor and interact using tcpdump and netcat. Examples:");
            println!("$ sudo tcpdump -i lo0 -s 0 -nnX");
            println!("$ nc -u localhost {}", port);
            println!("{}", serde_json::to_string(&LogMsg::Push::<RequestId, Value, ()>(1, 'X')).unwrap());
            println!("{}", serde_json::to_string(&LogMsg::Get::<RequestId, Value, ()>(2)).unwrap());
            println!();

            // WARNING: Omits `ordered_reliable_link` to keep the message
            //          protocol simple for `nc`.
            let id0 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 0));
            let id1 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 1));
            let id2 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 2));
            spawn(
                serde_json::to_vec,
                |bytes| serde_json::from_slice(bytes),
                vec![
                    (id0, LogPaxosActor::new( vec![id1, id2] )),
                    (id1, LogPaxosActor::new( vec![id0, id2] )),
                    (id2, LogPaxosActor::new( vec![id0, id1] )),
                ]).unwrap();
        }
        Some("spawnsingle") => {
            let port = 3000;

            let server_number : u8 = args.opt_free_from_str()?.unwrap();
            
            println!("  A single instance {} of a set of 3 servers that implement Log Paxos.", server_number);
            println!("  You can monitor and interact using tcpdump and netcat. Examples:");
            println!("$ sudo tcpdump -i lo0 -s 0 -nnX");
            println!("$ nc -u localhost {}", port);
            println!("{}", serde_json::to_string(&LogMsg::Push::<RequestId, Value, ()>(1, 'X')).unwrap());
            println!("{}", serde_json::to_string(&LogMsg::Get::<RequestId, Value, ()>(2)).unwrap());
            println!();

            // WARNING: Omits `ordered_reliable_link` to keep the message
            //          protocol simple for `nc`.
            let mut servers = HashableHashMap::<u8, Id>::new();
            servers.insert(0, Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 0)));
            servers.insert(1, Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 1)));
            servers.insert(2, Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 2)));
            
            let server_id = servers.remove(&server_number);
            spawn(
                serde_json::to_vec,
                |bytes| serde_json::from_slice(bytes),
                vec![
                    (server_id.unwrap(), LogPaxosActor::new( servers.values().cloned().collect()))
                ]).unwrap();
        }
        _ => {
            println!("USAGE:");
            println!("  ./logpaxos check [CLIENT_COUNT]");
            println!("  ./logpaxos explore [CLIENT_COUNT] [ADDRESS]");
            println!("  ./logpaxos spawn");
            println!("  ./logpaxos spawnsingle [server number]");
        }
    }

    Ok(())
}
