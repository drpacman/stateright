//! Defines an interface for log-like actors (via [`LogMsg`]) and also provides
//! [`LogActor`] for model checking.

#[cfg(doc)]
use crate::actor::ActorModel;
use crate::actor::{Actor, Envelope, Id, Out};
use crate::semantics::ConsistencyTester;
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use crate::semantics::append_only_log::{Log, LogOp, LogRet};

/// Defines an interface for a log-like actor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum LogMsg<RequestId, Value : Ord, InternalMsg> 
    where Value : Ord + Clone {
    /// A message specific to the register system's internal protocol.
    Internal(InternalMsg),

    /// Indicates that a value should be appended.
    Push(RequestId, Value),
    /// Indicates that a value should be retrieved.
    Get(RequestId),

    /// Indicates a successful `Put`. Analogous to an HTTP 2XX.
    PushOk(RequestId),
    /// Indicates a successful `Get`. Analogous to an HTTP 2XX.
    GetOk(RequestId, Log<Value>),
}
use LogMsg::*;

impl<RequestId, Value : Ord, InternalMsg> LogMsg<RequestId, Value, InternalMsg>
    where Value : Ord + Clone {
    /// This is a helper for configuring an [`ActorModel`] parameterized by a [`ConsistencyTester`]
    /// for its history. Simply pass this method to [`ActorModel::record_msg_out`]. Records
    /// [`LogOp::Get`] upon [`LogMsg::Get`] and [`LogOp::Push`] upon
    /// [`LogMsg::Push`].
    pub fn record_invocations<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&LogMsg<RequestId, Value, InternalMsg>>)
        -> Option<H>
    where H: Clone + ConsistencyTester<Id, Log<Value>>,
          Value: Clone + Debug + PartialEq,
    {
        // Currently throws away useful information about invalid histories. Ideally
        // checking would continue, but the property would be labeled with an error.
        if let Get(_) = env.msg {
            let mut history = history.clone();
            let _ = history.on_invoke(env.src, LogOp::Get);
            Some(history)
        } else if let Push(_req_id, value) = env.msg {
            let mut history = history.clone();
            let _ = history.on_invoke(env.src, LogOp::Push(value.clone()));
            Some(history)
        } else {
            None
        }
    }

    /// This is a helper for configuring an [`ActorModel`] parameterized by a [`ConsistencyTester`]
    /// for its history. Simply pass this method to [`ActorModel::record_msg_in`]. Records
    /// [`LogRet::GetOk`] upon [`LogMsg::GetOk`] and [`LogRet::PushOk`] upon
    /// [`LogMsg::PushOk`].
    pub fn record_returns<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&LogMsg<RequestId, Value, InternalMsg>>)
        -> Option<H>
    where H: Clone + ConsistencyTester<Id, Log<Value>>,
          Value: Clone + Debug + PartialEq,
    {
        // Currently throws away useful information about invalid histories. Ideally
        // checking would continue, but the property would be labeled with an error.
        match env.msg {
            GetOk(_, v) => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, LogRet::GetOk(v.contents()));
                Some(history)
            }
            PushOk(_) => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, LogRet::PushOk);
                Some(history)
            }
            _ => None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogActor<ServerActor> {
    /// A client that [`LogMsg::Push`]s a message and upon receving a
    /// corresponding [`LogMsg::PushOk`] follows up with a
    /// [`LogMsg::Get`].
    Client {
        put_count: usize,
        server_count: usize,
    },
    /// A server actor being validated.
    Server(ServerActor),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[derive(serde::Serialize)]
pub enum LogActorState<ServerState, RequestId> {
    /// A client that sends a sequence of [`LogMsg::Push`] messages before sending a
    /// [`LogMsg::Get`].
    Client {
        awaiting: Option<RequestId>,
        op_count: u64,
    },
    /// Wraps the state of a server actor.
    Server(ServerState),
}

// This implementation assumes the servers are at the beginning of the list of
// actors in the system under test so that an arbitrary server destination ID
// can be derived from `(client_id.0 + k) % server_count` for any `k`.
impl<ServerActor, InternalMsg> Actor for LogActor<ServerActor>
where
    ServerActor: Actor<Msg = LogMsg<u64, char, InternalMsg>>,
    InternalMsg: Clone + Debug + Eq + Hash,
{
    type Msg = LogMsg<u64, char, InternalMsg>;
    type State = LogActorState<ServerActor::State, u64>;

    #[allow(clippy::identity_op)]
    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        match self {
            LogActor::Client { put_count, server_count } => {
                let server_count = *server_count as u64;

                let index = id.0;
                if index < server_count {
                    panic!("LogActor clients must be added to the model after servers.");
                }

                if *put_count == 0 {
                    LogActorState::Client {
                        awaiting: None,
                        op_count: 0,
                    }
                } else {
                    let unique_request_id = 1 * index as u64; // next will be 2 * index
                    let value = (b'A' + (index - server_count) as u8) as char;
                    o.send(
                        Id((index + 0) % server_count),
                        Push(unique_request_id, value));
                    o.set_timer(Duration::from_millis(500)..Duration::from_millis(1000));
                    LogActorState::Client {
                        awaiting: Some(unique_request_id),
                        op_count: 1,
                    }
                }
            }
            LogActor::Server(server_actor) => {
                let mut server_out = Out::new();
                let state = LogActorState::Server(server_actor.on_start(id, &mut server_out));
                o.append(&mut server_out);
                state
            }
        }
    }

    fn on_msg(&self, id: Id, state: &mut Cow<Self::State>, src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        use LogActor as A;
        use LogActorState as S;

        match (self, &**state) {
            (
                A::Client { put_count, server_count },
                S::Client { awaiting: Some(awaiting), op_count },
            ) => {
                let server_count = *server_count as u64;
                o.cancel_timer();
                match msg {
                    LogMsg::PushOk(request_id) if &request_id == awaiting => {
                        let index = id.0;
                        let unique_request_id = ((op_count + 1) * index) as u64;
                        if *op_count < *put_count as u64 {
                            let value = (b'Z' - (index - server_count) as u8) as char;
                            o.send(
                                Id((index + op_count) % server_count),
                                Push(unique_request_id, value));
                            o.set_timer(Duration::from_millis(500)..Duration::from_millis(1000));                    
                        } else {
                            o.send(
                                Id((index + op_count) % server_count),
                                Get(unique_request_id));
                            o.set_timer(Duration::from_millis(500)..Duration::from_millis(1000));                    
                        }
                        *state = Cow::Owned(LogActorState::Client {
                            awaiting: Some(unique_request_id),
                            op_count: op_count + 1,
                        });
                    }
                    LogMsg::GetOk(request_id, _value) if &request_id == awaiting => {
                        *state = Cow::Owned(LogActorState::Client {
                            awaiting: None,
                            op_count: op_count + 1,
                        });
                    }
                    _ => {}
                }
            }
            (
                A::Server(server_actor),
                S::Server(server_state),
            ) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                // log::info!("Server msg {:?}", msg);
                server_actor.on_msg(id, &mut server_state, src, msg, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(LogActorState::Server(server_state))
                }
                o.append(&mut server_out);
            }
            _ => {}
        }
    }

    fn on_timeout(&self, id: Id, state: &mut Cow<Self::State>, out: &mut Out<Self>) {
        use LogActor as A;
        use LogActorState as S;
        match (self, &**state) {
            (
                A::Client { put_count: _, server_count: _ },
                S::Client { awaiting: Some(_awaiting), op_count },
            ) => {
                // log::info!("Timeout in log actor client");
                *state = Cow::Owned(LogActorState::Client {
                    awaiting: None,
                    op_count: op_count + 1,
                });
            },
            (
                A::Server(server_actor),
                S::Server(server_state),
            ) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                server_actor.on_timeout(id, &mut server_state, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(LogActorState::Server(server_state))
                }
                out.append(&mut server_out);
            },
            _ => {}
        }
    }
}

