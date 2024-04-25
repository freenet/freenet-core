## User Interface

On the normal web, a user might visit `https://gmail.com/`, their browser
will download the Gmail user interface which then runs in their browser and connects back to the Gmail servers. 

On Freenet the user interface is downloaded from a Freenet contract, and it
[interacts](overview.md) with contracts and delegates by sending messages
through the Freenet core.

![Delegate, Contrat, and UI Diagram](ui_delegate_contract.svg)

These UIs are built using web technologies such as HTML, CSS, and JavaScript,
and are distributed over Freenet and run in a web browser. UIs can create,
retrieve, and update contracts through a WebSocket connection to the local
Freenet peer, as well as communicate with delegates. 

Because UIs run in a web browser, they can be built using any web framework,
such as React, Angular, Vue.js, Bootstrap, and so on. 