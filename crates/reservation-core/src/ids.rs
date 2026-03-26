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

impl_id!(PoolId, u128);
impl_id!(HoldId, u128);
impl_id!(OperationId, u128);
impl_id!(ClientId, u128);
impl_id!(Lsn, u64);
impl_id!(Slot, u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientOperationKey {
    pub client_id: ClientId,
    pub operation_id: OperationId,
}

impl ClientOperationKey {
    #[must_use]
    pub const fn new(client_id: ClientId, operation_id: OperationId) -> Self {
        Self {
            client_id,
            operation_id,
        }
    }
}
