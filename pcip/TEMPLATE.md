<!--
RULES
* Never place a link to an external site like Google Doc. The proposal should be in this issue entirely.
* Use a spelling and grammar checker tools if available for you (there are plenty of free ones).

PROPOSAL HEALTH CHECK
I can read the design document and understand the problem statement and what you plan to change *without* resorting to a couple of hours of code reading just to start having a high level understanding of the change.

IMAGES
If you need the diagrams, please create a folder named pcip-XXX under the [pcip/static/img](https://github.com/apache/pulsar-java-contrib/tree/master/pcip/static/img) path and put the images in.

THIS COMMENTS
Please remove them when done.
-->

# PCIP-XXX: Proposal title

# Background knowledge

<!--
Describes all the knowledge you need to know in order to understand all the other sections in this PCIP

* Give a high level explanation on all concepts you will be using throughout this document. For example, if you want to talk about Persistent Subscriptions, explain briefly (1 paragraph) what this is. If you're going to talk about Transaction Buffer, explain briefly what this is. 
  If you're going to change something specific, then go into more detail about it and how it works. 
* Provide links where possible if a person wants to dig deeper into the background information. 

DON'T
* Do not include links *instead* explanation. Do provide links for further explanation.

EXAMPLES
* See [PCIP-2](https://github.com/apache/pulsar-java-contrib/pull/6), Background section to get an understanding on how you add the background knowledge needed.
  (They also included the motivation there, but ignore it as we place that in Motivation section explicitly).
-->

# Motivation

<!--
Describe the problem this proposal is trying to solve.

* Explain what is the problem you're trying to solve - current situation.
* This section is the "Why" of your proposal.
-->

# Goals

## In Scope

<!--
What this PCIP intend to achieve once It's integrated into Pulsar.
Why does it benefit Pulsar.
-->

## Out of Scope

<!--
Describe what you have decided to keep out of scope, perhaps left for a different PCIP/s.
-->


# High Level Design

<!--
Describe the design of your solution in *high level*.
Describe the solution end to end, from a birds-eye view.
Don't go into implementation details in this section.

I should be able to finish reading from beginning of the PCIP to here (including) and understand the feature and 
how you intend to solve it, end to end.

DON'T
* Avoid code snippets, unless it's essential to explain your intent.
-->

# Detailed Design

## Design & Implementation Details

<!--
This is the section where you dive into the details. It can be:
* Concrete class names and their roles and responsibility, including methods.
* Code snippets of existing code.
* Interface names and its methods.
* ...
-->

## Public-facing Changes

<!--
Describe the additions you plan to make for each public facing component. 
Remove the sections you are not changing.
Clearly mark any changes which are BREAKING backward compatability.
-->

### Public API
<!--
When adding a new endpoint to the REST API, please make sure to document the following:

* path
* query parameters
* HTTP body parameters, usually as JSON.
* Response codes, and for each what they mean.
  For each response code, please include a detailed description of the response body JSON, specifying each field and what it means.
  This is the place to document the errors.
-->

### Configuration

### CLI

# Get started

## Quick Start

<!--
Introduce how to use it and teach users how to use it quickly
-->