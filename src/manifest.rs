// use std::{borrow::Cow, ops::Deref};
//
// use arrow::{
//     array::{RecordBatch, RecordBatchReader},
//     datatypes::{Schema, SchemaRef},
// };
// use bincode_next::{
//     BorrowDecode, Decode, Encode,
//     de::Decoder,
//     enc::Encoder,
//     error::{DecodeError, EncodeError},
// };
//
// #[derive(Debug, Clone, Decode, Encode)]
// pub(crate) struct Entry<'a> {
//     version: u32,
//     context: Option<Context<'a>>,
//     schema: BinSchema,
//     compression: Option<Cow<'a, str>>,
//     shards: Vec<Shard<'a>>,
//     created: u64,
//     writer: Cow<'a, str>,
// }
//
// impl<'a> Entry<'a> {
//     const VERSION: u32 = 1;
//
//     pub(crate) fn new() -> Self {
//         Self {
//             version: Entry::VERSION,
//             context: None,
//             schema: BinSchema::empty(),
//             compression: None,
//             shards: vec![],
//             created: 0,
//             writer: Cow::default(),
//         }
//     }
//
//     pub(crate) fn add_schema<R>(&mut self, reader: R)
//     where
//         R: RecordBatchReader,
//     {
//         self.schema = self.schema.into();
//     }
//
//     pub(crate) fn add_ctx(&mut self, ctx: Context<'a>) {
//         self.context = Some(ctx);
//     }
//
//     pub(crate) fn add_timestamp(&mut self, value: u64) {
//         self.created = value
//     }
//
//     pub(crate) fn push_shard(&mut self, shard: Shard<'a>) {
//         self.shards.push(shard);
//     }
//
//     pub(crate) fn is_completed(&self) -> bool {
//         self.context.is_some() && !self.schema.fields.is_empty() && self.created > 0
//     }
//
//     pub(crate) fn ctx(&self) -> Option<&'a Context<'a>> {
//         self.context.as_ref()
//     }
// }
//
// #[derive(Debug, Clone, Decode, Encode)]
// pub(crate) struct Context<'a> {
//     key: Cow<'a, str>,
//     dag: Cow<'a, str>,
//     task: Cow<'a, str>,
//     run: Cow<'a, str>,
//     idx: i64,
// }
//
// impl<'a> Context<'a> {
//     pub(crate) fn new(
//         key: &'a str,
//         dag: &'a str,
//         task: &'a str,
//         run: &'a str,
//         idx: Option<i64>,
//     ) -> Self {
//         let idx = if let Some(idx) = idx {
//             idx
//         } else {
//             unreachable!("Cannot be None, yet Airflow contains it in the typedef")
//         };
//         Self {
//             key: key.into(),
//             dag: dag.into(),
//             task: task.into(),
//             run: run.into(),
//             idx,
//         }
//     }
//
//     pub(crate) fn as_bytes(&self) -> Vec<u8> {
//         let mut buf = Vec::with_capacity(64);
//         self.push_field(&mut buf, &self.dag);
//         self.push_field(&mut buf, &self.task);
//
//         buf.extend_from_slice(&((self.idx as u64) ^ (1 << 63).to_be_bytes()));
//         buf
//     }
//
//     fn push_field<W>(&self, buf: &mut Vec<u8>, value: &'a str) {
//         for b in value.as_bytes() {
//             if b == 0x00 {
//                 buf.extend_from_slice(&[0x00, 0xFF]);
//             } else {
//                 buf.push(*b);
//             }
//         }
//         buf.extend_from_slice(&[0x00, 0x00]);
//     }
// }
//
// #[derive(Debug, Clone, Decode, Encode)]
// pub(crate) struct Shard<'a> {
//     id: u32,
//     name: Cow<'a, str>,
//     rows: usize,
//     batch_sizes: Vec<usize>,
//     batch_order: Vec<u32>,
// }
//
// impl Shard<'_> {
//     pub(crate) fn new(id: u32) -> Self {
//         Self {
//             id,
//             name: Cow::Owned(format!("shard_{id:0>5}.arrow")),
//             rows: 0,
//             batch_sizes: vec![],
//             batch_order: vec![],
//         }
//     }
//
//     pub(crate) fn bump(&mut self, idx: u32, batch: &RecordBatch) {
//         self.batch_order.push(idx);
//         self.batch_sizes.push(batch.get_array_memory_size());
//         self.rows += batch.num_rows();
//     }
// }
//
// #[derive(Debug, Clone)]
// pub(crate) struct BinSchema(Schema);
//
// impl BinSchema {
//     pub(crate) fn empty() -> Self {
//         Self(Schema::empty())
//     }
// }
//
// impl Default for BinSchema {
//     fn default() -> Self {
//         Self::empty()
//     }
// }
//
// impl Deref for BinSchema {
//     type Target = Schema;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
//
// impl Encode for BinSchema {
//     fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
//         use arrow::ipc::convert::IpcSchemaEncoder;
//         use arrow::ipc::writer::DictionaryTracker;
//
//         let mut tracker = DictionaryTracker::new(true);
//         let fbb = IpcSchemaEncoder::new()
//             .with_dictionary_tracker(&mut tracker)
//             .schema_to_fb(&self);
//         let ipc_bytes = fbb.finished_data();
//
//         ipc_bytes.encode(encoder)?;
//         Ok(())
//     }
// }
//
// impl<Context> Decode<Context> for BinSchema {
//     fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
//         use arrow::ipc::convert::try_fb_to_schema;
//         use arrow::ipc::root_as_schema;
//
//         let ipc_bytes = Vec::<u8>::decode(decoder)?;
//         let ipc_schema = root_as_schema(ipc_bytes.as_slice())
//             .map_err(|e| DecodeError::OtherString(e.to_string()))?;
//         let schema = try_fb_to_schema(ipc_schema).unwrap();
//         Ok(BinSchema::from(schema))
//     }
// }
//
// bincode_next::impl_borrow_decode!(BinSchema);
//
// impl From<Schema> for BinSchema {
//     fn from(value: Schema) -> Self {
//         Self(value)
//     }
// }
//
// impl From<SchemaRef> for BinSchema {
//     fn from(value: SchemaRef) -> Self {
//         BinSchema((*value).clone())
//     }
// }
