use crate::book_order::BookOrder;
use crate::depth_updates::DepthUpdate;

pub struct UpdateProcessor {
    book: Option<BookOrder>,
}
