//! Validate-then-act tokens (design item M11.D39c).
//!
//! The rest of [`crate::state_backend`] answers "does this object agree with the job's
//! selector?" one object at a time. That is the right question, but it is not the claim a
//! destructive or publishing operation needs. Deleting a checkpoint's files, compacting its
//! operators, publishing a generation, rewriting a checkpoint's metadata, and collecting a
//! history are all operations over a *whole* object — every epoch, every operator, the
//! program's exact operator set, the objects that have to be present — and each of them has
//! a first irreversible effect. Checking one item, acting on it, and then checking the next
//! means the last item's disagreement arrives after the first item's effect has already
//! happened, which is the shape of PR-#157 rounds 1–4.
//!
//! Those call sites were fixed one at a time in M11.T08 by moving the checks in front of the
//! effects. This module makes the discipline unforgeable instead of remembered: whole-object
//! validation produces a [`Validated<T>`], and the operations that act take only the token.
//! A new call site cannot delete, compact, publish, or rewrite without first producing one,
//! because there is no other way to name the value the operation needs.
//!
//! This is the one M11.T25 mechanism that is live on the production path — the rest of that
//! todo is built behind the controller's internal `LifecycleMode` seam and is not selected —
//! so it is worth saying plainly what it costs to deploy: nothing. The
//! discipline is entirely in-process. No stored object gains a field, changes its encoding or
//! moves, and no message on the wire mentions a token; a checkpoint written before this change
//! validates and restores exactly as it did, and one written after it is byte-identical. What
//! changed is which *call sites* can name the value an operation needs.
//!
//! # Why the token cannot be forged
//!
//! [`Validated<T>`] holds a private field, has no public constructor, and deliberately
//! implements none of `From<T>`, `Default`, `Clone`, or `Deserialize`. Its single
//! constructor is [`Validated::validate`], which is available only for `T: WholeObject` and
//! which *runs* [`WholeObject::check_whole`] before it wraps anything. Unwrapping a token is
//! free ([`Validated::get`], [`Validated::into_inner`]) because unwrapping proves nothing;
//! wrapping is what is restricted.
//!
//! Implementing [`WholeObject`] for a type of your own does not help: the impl that runs is
//! the one belonging to the `T` you are trying to wrap, so a `Validated<CheckpointCleanup>`
//! can only come from `CheckpointCleanup`'s own check. The trait is therefore left
//! implementable — each of the five call-site families implements it beside the code that
//! owns the object, rather than in one central module that would have to know all of them.
//!
//! # Writing a `WholeObject` impl
//!
//! [`WholeObject::check_whole`] must be a complete statement about the value, not a
//! restatement of what its constructor happened to do. It runs on a value the caller may
//! have built by hand, so anything the operation relies on — that every epoch in the range
//! is described, that the operator set is exactly the program's, that no required object is
//! absent — has to be checked here. A check that only re-reads a flag the constructor set
//! proves nothing, because the token would then be as forgeable as the flag.

use std::fmt;

/// A `T` whose whole contents have been checked, carried as proof that the check ran.
///
/// Produced only by [`Validated::validate`]. See the [module docs](self) for why the
/// operations that act on checkpoints, generations, and metadata take this rather than a
/// bare `T`, and for what makes it unforgeable.
pub struct Validated<T>(T);

/// A value whose validity is a claim about the *whole* of it.
///
/// Implemented beside the code that owns the object, so that the check and the thing being
/// checked stay in the same place. See the [module docs](self) for what an implementation
/// owes its callers.
pub trait WholeObject: Sized {
    /// Everything the check needs that the value does not carry itself — typically the
    /// job's [`StateBackendSelector`](super::StateBackendSelector) and, where exact program
    /// coverage is part of the claim, the operator set the job's workers will build.
    ///
    /// Generic over a lifetime so a context can borrow the caller's program set instead of
    /// forcing a copy of it per check.
    type Context<'a>;

    /// What a failed check reports. Each family keeps the error type its callers already
    /// handle, so introducing a token changes no caller's error handling.
    type Error;

    /// Checks the whole value, reporting the first thing that makes it unusable.
    ///
    /// Runs before any effect and performs no I/O: everything it needs is either already in
    /// the value or handed in through `context`. That is what lets the check be complete —
    /// a check that had to read would be back to reading item by item.
    fn check_whole(&self, context: Self::Context<'_>) -> Result<(), Self::Error>;
}

impl<T: WholeObject> Validated<T> {
    /// Checks `value` as a whole and, only if it passes, wraps it in the token.
    ///
    /// This is the only way a [`Validated<T>`] comes into existence.
    ///
    /// # Errors
    ///
    /// Returns whatever [`WholeObject::check_whole`] reported. Nothing is wrapped and, since
    /// no operation can run without the token, nothing has been acted on.
    pub fn validate(value: T, context: T::Context<'_>) -> Result<Self, T::Error> {
        value.check_whole(context)?;
        Ok(Self(value))
    }
}

impl<T> Validated<T> {
    /// Borrows the checked value.
    pub fn get(&self) -> &T {
        &self.0
    }

    /// Takes the checked value back out.
    ///
    /// Unwrapping is unrestricted on purpose: the token's guarantee is about how it was
    /// obtained, so handing the value back cannot weaken it. What a caller cannot do is put
    /// a value *in* without [`Validated::validate`].
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for Validated<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Validated").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{Validated, WholeObject};
    use std::cell::Cell;

    /// A whole object whose check counts its own runs, so a test can tell "the check ran"
    /// from "the value happened to be fine".
    #[derive(Debug)]
    struct Counted {
        ok: bool,
    }

    thread_local! {
        static CHECKS: Cell<u32> = const { Cell::new(0) };
    }

    impl WholeObject for Counted {
        type Context<'a> = &'a str;
        type Error = String;

        fn check_whole(&self, context: &str) -> Result<(), String> {
            CHECKS.with(|c| c.set(c.get() + 1));
            if self.ok {
                Ok(())
            } else {
                Err(format!("{context} is not whole"))
            }
        }
    }

    /// The token's whole contract in one place: the check runs on every construction, a
    /// failing check yields no token, and unwrapping does not launder anything back in.
    #[test]
    fn a_token_exists_only_where_the_whole_check_ran_and_passed() {
        CHECKS.with(|c| c.set(0));

        let token = Validated::validate(Counted { ok: true }, "the plan").unwrap();
        assert_eq!(CHECKS.with(Cell::get), 1);
        assert!(token.get().ok);

        let err = Validated::validate(Counted { ok: false }, "the plan").unwrap_err();
        assert_eq!(err, "the plan is not whole");
        assert_eq!(CHECKS.with(Cell::get), 2, "the check must run every time");

        // Unwrapping returns the value, and the only way back to a token is another check.
        let value = token.into_inner();
        let _ = Validated::validate(value, "the plan").unwrap();
        assert_eq!(CHECKS.with(Cell::get), 3);
    }
}
