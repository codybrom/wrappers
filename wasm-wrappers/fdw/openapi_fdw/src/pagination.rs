//! Pagination state tracking and loop detection

/// A pagination token: a cursor string, a full/partial URL, or a page number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaginationToken {
    /// Token-based pagination (e.g., Stripe next_cursor)
    Cursor(String),
    /// Link-based pagination (e.g., GitHub Link header, HAL _links)
    Url(String),
    /// Page-number pagination: the number of the page to request next.
    ///
    /// For APIs that return neither a next URL nor a cursor value, and instead
    /// expect a page number to be incremented while a boolean elsewhere in the
    /// response says more remains (e.g. Zoho's `info.more_records`). Opt-in
    /// only, via the `page_param` + `has_more_path` options.
    Page(u32),
}

impl PaginationToken {
    /// Returns the inner cursor string, or None for other kinds.
    pub(crate) fn as_cursor(&self) -> Option<&str> {
        match self {
            Self::Cursor(s) => Some(s),
            Self::Url(_) | Self::Page(_) => None,
        }
    }

    /// Returns the inner URL string, or None for other kinds.
    pub(crate) fn as_url(&self) -> Option<&str> {
        match self {
            Self::Url(s) => Some(s),
            Self::Cursor(_) | Self::Page(_) => None,
        }
    }

    /// Returns the inner page number, or None for other kinds.
    pub(crate) fn as_page(&self) -> Option<u32> {
        match self {
            Self::Page(n) => Some(*n),
            Self::Cursor(_) | Self::Url(_) => None,
        }
    }
}

/// Tracks pagination state across pages within a single scan.
///
/// Detects infinite loops (duplicate token) and enforces page limits.
#[derive(Debug, Default)]
pub(crate) struct PaginationState {
    /// Token for the next page (cursor or URL)
    pub(crate) next: Option<PaginationToken>,
    /// Token from the previous page (for loop detection)
    pub(crate) previous: Option<PaginationToken>,
    /// Number of pages fetched so far
    pub(crate) pages_fetched: usize,
}

impl PaginationState {
    /// Reset all pagination state for a new scan.
    pub(crate) fn reset(&mut self) {
        self.next = None;
        self.previous = None;
        self.pages_fetched = 0;
    }

    /// Returns true when there are no more pages to fetch.
    pub(crate) fn is_exhausted(&self) -> bool {
        self.next.is_none()
    }

    /// Detect a pagination loop (duplicate token).
    ///
    /// Returns a human-readable reason if a loop is detected.
    pub(crate) fn detect_loop(&self) -> Option<&'static str> {
        match (&self.next, &self.previous) {
            (Some(PaginationToken::Cursor(n)), Some(PaginationToken::Cursor(p))) if n == p => {
                Some("duplicate cursor detected (possible infinite loop)")
            }
            (Some(PaginationToken::Url(n)), Some(PaginationToken::Url(p))) if n == p => {
                Some("duplicate URL detected (possible infinite loop)")
            }
            // Page numbers are derived by incrementing, so a repeat should be
            // unreachable — treated as a loop anyway rather than trusted.
            (Some(PaginationToken::Page(n)), Some(PaginationToken::Page(p))) if n == p => {
                Some("duplicate page number detected (possible infinite loop)")
            }
            _ => None,
        }
    }

    /// Returns true if the page limit has been reached.
    pub(crate) fn exceeds_limit(&self, max_pages: usize) -> bool {
        self.pages_fetched >= max_pages
    }

    /// Save current next value as previous (for loop detection) and increment page count.
    ///
    /// Call this before fetching each subsequent page.
    pub(crate) fn advance(&mut self) {
        self.previous = self.next.clone();
        self.pages_fetched += 1;
    }

    /// Record the first page after initial make_request in begin_scan.
    ///
    /// Only sets pages_fetched = 1. Does NOT copy next into previous --
    /// there was no token sent for the first page, so previous must stay
    /// None to avoid a false-positive loop detection.
    pub(crate) fn record_first_page(&mut self) {
        self.pages_fetched = 1;
    }

    /// Clear next-page token (e.g., on 404 or empty response).
    pub(crate) fn clear_next(&mut self) {
        self.next = None;
    }
}

#[cfg(test)]
#[path = "pagination_tests.rs"]
mod tests;
