<<<<<<< before updating
# Contributing

## Getting Started

* Make sure you have a [GitHub account](https://github.com/signup/free)
* Submit a ticket for your issue, assuming one does not already exist.
  * Clearly describe the issue including steps to reproduce when it is a bug.
  * Make sure you fill in the earliest version that you know has the issue.
* Fork the repository on GitHub


## Making Changes

* Create a topic branch from where you want to base your work.
  * This is usually the main branch.
  * Only target release branches if you are certain your fix must be on that
    branch.
  * To quickly create a topic branch based on main; `git checkout -b
    fix/main/my_contribution main`. Please avoid working directly on the
    `main` branch.
* Make commits of logical units.
* Check for unnecessary whitespace with `git diff --check` before committing.
* Make sure your commit messages are in the proper format (see below)
* Make sure you have added the necessary tests for your changes.
* Run _all_ the tests to assure nothing else was accidentally broken.

### Writing the commit message

Commit messages should be clear and follow a few basic rules. Example:

```
ENH: add functionality X to event_model.<submodule>.

The first line of the commit message starts with a capitalized acronym
(options listed below) indicating what type of commit this is.  Then a blank
line, then more text if needed.  Lines shouldn't be longer than 72
characters.  If the commit is related to a ticket, indicate that with
"See #3456", "See ticket 3456", "Closes #3456" or similar.
```

Describing the motivation for a change, the nature of a bug for bug fixes 
or some details on what an enhancement does are also good to include in a 
commit message. Messages should be understandable without looking at the code 
changes. 

Standard acronyms to start the commit message with are:
```
API: an (incompatible) API change
BLD: change related to building numpy
BUG: bug fix
CI : continuous integration
DEP: deprecate something, or remove a deprecated object
DEV: development tool or utility
DOC: documentation
ENH: enhancement
MNT: maintenance commit (refactoring, typos, etc.)
REV: revert an earlier commit
STY: style fix (whitespace, PEP8)
TST: addition or modification of tests
REL: related to releases
```
## The Pull Request

* Now push to your fork
* Submit a [pull request](https://help.github.com/articles/using-pull-requests) to this branch. This is a start to the conversation.

At this point you're waiting on us. We like to at least comment on pull requests within three business days 
(and, typically, one business day). We may suggest some changes or improvements or alternatives.

Hints to make the integration of your changes easy (and happen faster):
- Keep your pull requests small
- Don't forget your unit tests
- All algorithms need documentation, don't forget the .rst file
- Don't take changes requests to change your code personally
=======
# Contribute to the project

Contributions and issues are most welcome! All issues and pull requests are
handled through [GitHub](https://github.com/bluesky/event-model/issues). Also, please check for any existing issues before
filing a new one. If you have a great idea but it involves big changes, please
file a ticket before making a pull request! We want to make sure you don't spend
your time coding something that might not fit the scope of the project.

## Issue or Discussion?

Github also offers [discussions](https://github.com/bluesky/event-model/discussions) as a place to ask questions and share ideas. If
your issue is open ended and it is not obvious when it can be "closed", please
raise it as a discussion instead.

## Code Coverage

While 100% code coverage does not make a library bug-free, it significantly
reduces the number of easily caught bugs! Please make sure coverage remains the
same or is improved by a pull request!

## Developer Information

It is recommended that developers use a [vscode devcontainer](https://code.visualstudio.com/docs/devcontainers/containers). This repository contains configuration to set up a containerized development environment that suits its own needs.

This project was created using the [Diamond Light Source Copier Template](https://github.com/DiamondLightSource/python-copier-template) for Python projects.

For more information on common tasks like setting up a developer environment, running the tests, and setting a pre-commit hook, see the template's [How-to guides](https://diamondlightsource.github.io/python-copier-template/2.3.0/how-to.html).
>>>>>>> after updating
