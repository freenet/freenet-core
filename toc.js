// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded "><a href="introduction.html"><strong aria-hidden="true">1.</strong> Introduction</a></li><li class="chapter-item expanded affix "><li class="part-title">Components</li><li class="chapter-item expanded "><a href="components/overview.html"><strong aria-hidden="true">2.</strong> Overview</a></li><li class="chapter-item expanded "><a href="components/contracts.html"><strong aria-hidden="true">3.</strong> Contracts</a></li><li class="chapter-item expanded "><a href="components/delegates.html"><strong aria-hidden="true">4.</strong> Delegates</a></li><li class="chapter-item expanded "><a href="components/ui.html"><strong aria-hidden="true">5.</strong> User Interfaces</a></li><li class="chapter-item expanded affix "><li class="part-title">Architecture</li><li class="chapter-item expanded "><a href="architecture/p2p-network.html"><strong aria-hidden="true">6.</strong> P2P Network</a></li><li class="chapter-item expanded "><a href="architecture/irouting.html"><strong aria-hidden="true">7.</strong> Intelligent Routing</a></li><li class="chapter-item expanded "><a href="architecture/transport.html"><strong aria-hidden="true">8.</strong> Transport</a></li><li class="chapter-item expanded affix "><li class="part-title">Developer Guide</li><li class="chapter-item expanded "><a href="tutorial.html"><strong aria-hidden="true">9.</strong> Tutorial: Create an App</a></li><li class="chapter-item expanded "><a href="contract-interface.html"><strong aria-hidden="true">10.</strong> Contract interfaces</a></li><li class="chapter-item expanded "><a href="manifest.html"><strong aria-hidden="true">11.</strong> freenet.toml format</a></li><li class="chapter-item expanded affix "><li class="part-title">Examples</li><li class="chapter-item expanded "><a href="examples/antiflood-tokens.html"><strong aria-hidden="true">12.</strong> Antiflood Tokens</a></li><li class="chapter-item expanded "><a href="examples/blind-trust-tokens.html"><strong aria-hidden="true">13.</strong> Blind Trust Tokens</a></li><li class="chapter-item expanded affix "><li class="part-title">Community and Support</li><li class="chapter-item expanded "><a href="community.html"><strong aria-hidden="true">14.</strong> Community</a></li><li class="chapter-item expanded affix "><li class="part-title">Reference</li><li class="chapter-item expanded "><a href="glossary.html"><strong aria-hidden="true">15.</strong> Glossary</a></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
