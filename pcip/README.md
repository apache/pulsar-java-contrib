# Pulsar Java Contrib Improvement Proposal (PCIP)

## What is a PCIP?

The PCIP is a "Pulsar Java Contrib Improvement Proposal" and it's the mechanism used to propose changes to the Apache Pulsar Java Contrib codebases.

The changes might be in terms of new features, large code refactoring.

In practical terms, the PCIP defines a process in which developers can submit a design doc, receive feedback and get the "go ahead" to execute.


### What is the goal of a PCIP?

There are several goals for the PCIP process:

1. As a user manual, add instructions when introducing new features or modifying existing features. 
2. Explain the functional design ideas to facilitate review and later maintenance.

It is not a goal for PCIP to add undue process or slow-down the development.

### When is a PCIP required?

* Any new feature for Pulsar Java Contrib
* Any change to the semantic of existing functionality
* Any large code change that will touch multiple components
* Any change to the configuration

### When is a PCIP *not* required?

* Bug-fixes
* Documentation changes

### Who can create a PCIP?

Any person willing to contribute to the Apache Pulsar Java Contrib project is welcome to create a PCIP.

## How does the PCIP process work?

The process works in the following way:

1. Fork https://github.com/apache/pulsar-java-contrib repository (Using the fork button on GitHub).
2. Clone the repository, and on it, copy the file `pcip/TEMPLATE.md` and name it `pcip-xxx.md`. The number `xxx` should be the next sequential number after the last contributed PCIP. You view the list of contributed PIPs (at any status) as a list of Pull Requests having a "PCIP" label. Use the link [here](https://github.com/apache/pulsar-java-contrib/pulls?q=is%3Apr+%22PCIP-%22+in%3Atitle+sort%3Acreated-desc) as shortcut.
3. Write the proposal following the section outlined by the template and the explanation for each section in the comment it contains (you can delete the comment once done).
   * If you need the diagrams, please create a folder named pcip-XXX under the [pcip/static/img](https://github.com/apache/pulsar-java-contrib/tree/master/pcip/static/img) path and put the images in.
4. Create GitHub Pull request (PR). The PR title should be `[improve][pcip] PCIP-xxx: {title}`, where the `xxx` match the number given in previous step (file-name). Replace `{title}` with a short title to your proposal.
   *Validate* again that your number does not collide, by step (2) numbering check.
5. Based on the discussion and feedback, some changes might be applied by authors to the text of the proposal. They will be applied as extra commits, making it easier to track the changes.

To speed up the development process:
1. you can put the code changes and design documents(pcip/pcip-xxx.md) in one PR. 
2. If the code is not yet developed, you can include only a design document(pcip/pcip-xxx.md) in the PR and submit the code separately after the development is completed.

## List of PCIPs

### Historical PCIPs
You can the view list of PCIPs previously managed by GitHub [here](https://github.com/apache/pulsar-java-contrib/tree/master/pcip)

### List of PCIPs
1. You can view all PCIPs (besides the historical ones) as the list of Pull Requests having title starting with `PCIP-`. Here is the [link](https://github.com/apache/pulsar-java-contrib/pulls?q=is%3Apr+%22PCIP-%22+in%3Atitle+sort%3Acreated-desc) for it.
    - Merged PR means the PCIP was accepted.
    - Closed PR means the PCIP was rejected.
    - Open PR means the PCIP was submitted and is in the process of discussion.
2. You can also take a look at the file in the `pcip` folder. Each one is an approved PCIP.