use std::collections::HashMap;

use futures::Stream;
use niri_ipc::{Action, Event, Output, Reply, Request, Workspace, socket::Socket};
pub use state::{Snapshot, Window};
pub use window_stream::WindowStream;

use crate::error::Error;

mod reply;
mod state;
mod window_stream;

/// The top level client for Niri.
#[derive(Debug, Clone, Copy)]
pub struct Niri {}

impl Niri {
    pub fn new() -> Self {
        // Since niri_ipc is essentially stateless, we don't maintain anything much here.
        Self {}
    }

    /// Requests that the given window ID should be activated.
    #[tracing::instrument(level = "TRACE", err)]
    pub fn activate_window(&self, id: u64) -> Result<(), Error> {
        let reply = request(Request::Action(Action::FocusWindow { id }))?;
        reply::typed!(Handled, reply)
    }

    /// Requests that the given window ID should be closed.
    #[tracing::instrument(level = "TRACE", err)]
    pub fn close_window(&self, id: u64) -> Result<(), Error> {
        let reply = request(Request::Action(Action::CloseWindow { id: Some(id) }))?;
        reply::typed!(Handled, reply)
    }

    /// Returns the current outputs.
    pub fn outputs(&self) -> Result<HashMap<String, Output>, Error> {
        let reply = request(Request::Outputs)?;
        reply::typed!(Outputs, reply)
    }

    /// Returns a stream of window snapshots.
    pub fn window_stream(&self) -> WindowStream {
        WindowStream::new()
    }

    /// Returns a stream of workspace changes.
    pub fn workspace_stream(&self) -> Result<impl Stream<Item = Vec<Workspace>> + use<>, Error> {
        let (tx, rx) = async_channel::unbounded();

        std::thread::spawn(move || {
            if let Err(e) = workspace_stream_task(tx) {
                tracing::error!(%e, "Niri taskbar workspace stream error");
            }
        });

        Ok(rx)
    }
}

fn workspace_stream_task(tx: async_channel::Sender<Vec<Workspace>>) -> Result<(), Error> {
    let mut socket = socket()?;
    let reply = socket.send(Request::EventStream).map_err(Error::NiriIpc)?;
    reply::typed!(Handled, reply)?;

    let mut next = socket.read_events();
    loop {
        match next() {
            Ok(Event::WorkspacesChanged { workspaces }) => {
                if tx.send_blocking(workspaces).is_err() {
                    break;
                }
            }
            Ok(_) => (),
            Err(e) => {
                tracing::error!(%e, "Niri IPC error reading from event stream");
                return Err(Error::NiriIpc(e));
            }
        }
    }

    Ok(())
}

// Helper to marshal request errors into our own type system.
//
// This can't be used for event streams, since the stream callback is thrown away in this function.
#[tracing::instrument(level = "TRACE", err)]
fn request(request: Request) -> Result<Reply, Error> {
    socket()?.send(request).map_err(Error::NiriIpc)
}

// Helper to connect to the Niri socket.
#[tracing::instrument(level = "TRACE", err)]
fn socket() -> Result<Socket, Error> {
    Socket::connect().map_err(Error::NiriIpc)
}
