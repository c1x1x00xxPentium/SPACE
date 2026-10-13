//! Server-speaks-first TCP under TLS interception.
//!
//! Interception needs a ClientHello, but FTP, SMTP, POP3 and IMAP clients wait
//! silently for the server's greeting. For a silent guest whose destination
//! was admitted without DNS evidence, [`probe`] dials the destination and
//! waits for one side to speak:
//!
//! - Destination first: the flow is relayed opaquely, greeting included.
//! - Guest first (data or EOF): the probe connection is dropped unwritten and
//!   the guest's bytes are admitted as usual, so a late ClientHello is still
//!   inspected or refused.
//! - Neither within [`PROBE_WINDOW`]: the flow is refused as before.
//!
//! No guest byte reaches the destination before it speaks. After a greeting
//! the session is opaque, so a later STARTTLS is not inspected, as with
//! interception off for the same address-allowed destination.

use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use super::transport;

/// How long the probe waits, from the dial, for either side to speak. It
/// matches the dial's own bound and leaves room for servers that delay their
/// greeting.
pub(super) const PROBE_WINDOW: Duration = Duration::from_secs(10);

/// The most the probe reads of the destination's first write; the relay
/// carries the rest.
const GREETING_READ_BYTES: usize = 4096;

pub(super) enum Probe<U> {
    /// The destination wrote while the guest was silent.
    ServerFirst {
        upstream: U,
        greeting: Vec<u8>,
    },
    /// The guest wrote or closed first; the probe connection was dropped
    /// unwritten.
    GuestFirst,
    /// Neither side spoke within the window.
    TimedOut,
    /// The destination closed or failed before speaking.
    UpstreamClosed,
    ConnectFailed(transport::Error),
    /// Shutdown or a policy resweep cancelled the flow.
    Cancelled,
}

/// Dial `dst` and wait for the first of: cancellation, the guest speaking or
/// closing, the destination speaking, or `window` elapsing. The guest wins a
/// tie. Writes nothing to the destination and consumes nothing from the guest.
pub(super) async fn probe<F, Fut, U>(
    client: &TcpStream,
    dst: SocketAddr,
    connect: F,
    window: Duration,
    cancellation: &CancellationToken,
) -> Probe<U>
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = Result<U, transport::Error>>,
    U: AsyncRead + Unpin,
{
    let destination_speaks = async {
        let mut upstream = match connect(dst).await {
            Ok(upstream) => upstream,
            Err(error) => return Probe::ConnectFailed(error),
        };
        let mut greeting = vec![0_u8; GREETING_READ_BYTES];
        match upstream.read(&mut greeting).await {
            Ok(0) | Err(_) => Probe::UpstreamClosed,
            Ok(read) => {
                greeting.truncate(read);
                Probe::ServerFirst { upstream, greeting }
            }
        }
    };
    // A peek consumes nothing, so the ClientHello scan still sees the bytes.
    let mut first_byte = [0_u8; 1];
    let guest_speaks = client.peek(&mut first_byte);
    let settled = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Probe::Cancelled,
        _ = guest_speaks => return Probe::GuestFirst,
        probe = destination_speaks => probe,
        () = tokio::time::sleep(window) => Probe::TimedOut,
    };
    // Tokio may not have seen the guest's bytes yet when the destination
    // settles, so ask the socket before letting the destination win.
    if guest_has_spoken(client) {
        Probe::GuestFirst
    } else {
        settled
    }
}

/// Whether the guest has sent data or closed, read from the socket itself
/// rather than Tokio's readiness. Any error other than "nothing yet" counts,
/// so the flow takes the guest-first path, which fails closed.
fn guest_has_spoken(client: &TcpStream) -> bool {
    let mut byte = [MaybeUninit::<u8>::uninit()];
    // The socket is non-blocking, so this returns at once.
    loop {
        match socket2::SockRef::from(client).peek(&mut byte) {
            Ok(_) => return true,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return error.kind() != io::ErrorKind::WouldBlock,
        }
    }
}
