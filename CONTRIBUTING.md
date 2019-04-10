# Contributing to Fink


To start, it helps to announce what you are working on ahead of time to
confirm that it is an acceptable contribution. To do this open an issue/PR or send
an email to peloton at lal dot in2p3 dot fr.

## Issues

Feel free to submit issues and enhancement requests.

## Contributing

In general, we appreciate the "fork-and-pull" Git workflow.

* Fork the repo on GitHub
* Clone the project to your own machine and set up a new branch
* Commit changes to your branch
* Push your work back up to your fork
* Submit a Pull request so that we can review your changes
* Label your PR to help the tracking and prioritisation of your contribution.

NOTE: Be sure to merge the latest from "upstream" before making a pull request!

## Branch naming

Using some form of convention to define the name of a branch for your PR is always a good idea. We do not have strong preference. For reference, here are some conventions:

- Relating to a particular issue: `issue/issue-number/few-word-description`
- Relating to a particular user: `u/your-username/few-word-description`

Feel free to pick up one or another!

## Code style

Make sure the new code to be introduced is documented and has been tested.
Please include the corresponding unit test with your code, and do not hesitate
to have a high verbosity in your comments ;-)
We tend to reject large contributions without any comments or tests.
Also make sure your new contribution does not break the current test suite
(or explain why we did a mistake!).
