# Data Pipeline Guidance (with Apache Storm)
_Microsoft patterns & practices_

This project focuses on using Apache Storm/Trident with Java. For guidance on
using .NET _without_ Storm, see the companion
[Data Pipeline Guidance](https://github.com/mspnp/data-pipeline).

##Overview

The two primary concerns of this project are:

* Facilitating cold storage of data for later analytics.
That is, translating the _chatty_ stream of events into _chunky_ blobs.

* Demonstrate how to use OpaqueTridentEventHubSpout and
Apache Storm/Trident to store Microsoft Azure Eventhub messages to Microsoft Azure
Blob exactly-once.

## Next Steps

* [Architecture Overview](/docs/ArchitectureOverview.md)
* [Getting Started](/docs/GettingStarted.md)
* [Step by Step Walk Through](/docs/step-by-step-walkthrough.md)
