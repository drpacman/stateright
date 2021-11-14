//! Implements [`SequentialSpec`] for [`Log`] operational semantics.
use crate::semantics::SequentialSpec;
use serde::{ Serialize, Deserialize};
use std::cmp::{ Ord, Ordering, PartialOrd };
/// An operation that can be invoked upon an append only Log, resulting in a
/// [`LogRet`].
#[derive(Clone, Debug, Hash, PartialEq)]
pub enum LogOp<T> { Push(T), Get, Len }

/// A return value for a [`LogOp`] invoked upon a [`Log`].
#[derive(Clone, Debug, Hash, PartialEq)]
pub enum LogRet<T> { PushOk, GetOk(Vec<T>), LenOk(usize) }

#[derive(Default, Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Log<T: Ord + Clone> {
    contents : Vec<T>
}

impl<T : Ord + Clone> Log<T>  {
    pub fn new() -> Log<T> {
        Log {
            contents : Vec::new()
        }
    }

    pub fn from(contents: Vec<T>) -> Log<T> {
        Log { contents }
    }

    pub fn contents(&self) -> Vec<T> {
        self.contents.clone()
    }

    pub fn append(mut self, value : T) -> Self {
        self.contents.push(value);
        self
    }

    pub fn extends(&self, log : &Log<T>) -> bool {
        self.contents().starts_with(&log.contents()[..])
    }
}

impl<T> Ord for Log<T>
    where T : Clone + Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.contents.cmp(&other.contents)
    }
}

impl<T> PartialOrd for Log<T> 
    where T : Ord + Clone {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.contents.partial_cmp(&other.contents)
    }
}

impl<T : Ord> SequentialSpec for Log<T>
    where T: Clone + PartialEq
{
    type Op = LogOp<T>;
    type Ret = LogRet<T>;
    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            LogOp::Push(v) => {
                self.contents.push(v.clone());
                LogRet::PushOk
            }
            LogOp::Get => LogRet::GetOk(self.contents.clone()),
            LogOp::Len => LogRet::LenOk(self.contents.len()),
        }
    }
    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        match (op, ret) {
            (LogOp::Push(v), LogRet::PushOk) => {
                self.contents.push(v.clone());
                true
            }
            (LogOp::Get, LogRet::GetOk(v)) => {
                &self.contents == v
            }
            (LogOp::Len, LogRet::LenOk(l)) => {
                &self.contents.len() == l
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn models_expected_semantics() {
        let mut log = Log::<char>::new();
        assert_eq!(log.invoke(&LogOp::Len),       LogRet::LenOk(0));
        assert_eq!(log.invoke(&LogOp::Push('A')), LogRet::PushOk);
        assert_eq!(log.invoke(&LogOp::Len),       LogRet::LenOk(1));
        assert_eq!(log.invoke(&LogOp::Push('B')), LogRet::PushOk);
        assert_eq!(log.invoke(&LogOp::Len),       LogRet::LenOk(2));
        assert_eq!(log.invoke(&LogOp::Get),       LogRet::GetOk(vec!['A','B']));      
    }

    #[test]
    fn accepts_valid_histories() {
        assert!(Log::<isize>::new().is_valid_history(vec![]));
        assert!(Log::new().is_valid_history(vec![
            (LogOp::Push(10), LogRet::PushOk),
            (LogOp::Push(20), LogRet::PushOk),
            (LogOp::Len, LogRet::LenOk(2)),
            (LogOp::Get, LogRet::GetOk(vec![10,20]))            
        ]));
    }

    #[test]
    fn rejects_invalid_histories() {
        assert!(!Log::new().is_valid_history(vec![
            (LogOp::Push(10), LogRet::PushOk),
            (LogOp::Push(20), LogRet::PushOk),
            (LogOp::Len, LogRet::LenOk(1)),
            (LogOp::Push(30), LogRet::PushOk),
        ]));
        assert!(!Log::new().is_valid_history(vec![
            (LogOp::Push(10), LogRet::PushOk),
            (LogOp::Push(20), LogRet::PushOk),
            (LogOp::Get, LogRet::GetOk(vec![10]))
        ]));
    }
}
