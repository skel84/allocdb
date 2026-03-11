macro_rules! impl_id {
    ($name:ident, $inner:ty) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(pub $inner);

        impl $name {
            #[must_use]
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }
    };
}

impl_id!(ResourceId, u128);
impl_id!(ReservationId, u128);
impl_id!(HolderId, u128);
impl_id!(OperationId, u128);
impl_id!(ClientId, u128);
impl_id!(Lsn, u64);
impl_id!(Slot, u64);
