<img src="https://raw.githubusercontent.com/bluesky/event-model/refs/heads/main/docs/images/event-model-logo.svg"
     style="background: none" width="120px" height="120px" align="center" alt="logo">

[![CI](https://github.com/bluesky/event-model/actions/workflows/ci.yml/badge.svg)](https://github.com/bluesky/event-model/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/bluesky/event-model/branch/main/graph/badge.svg)](https://codecov.io/gh/bluesky/event-model)
[![PyPI](https://img.shields.io/pypi/v/event-model.svg)](https://pypi.org/project/event-model)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Data model used by the bluesky ecosystem.

# Event Model

A primary design goal of bluesky is to enable better research by recording
rich metadata alongside measured data for use in later analysis. Documents are
how we do this.

This repository contains the formal schemas for bluesky's streaming data model
and some Python tooling for composing, validating, and transforming documents
in the model.

## Where is my data?

For the full details and schema please see the `data_model` section.  This is a very quick guide to where
you should look for / put different kinds of information

* Information about your sample that you know before the measurement → *Start* Document
* What experiment you intended to do → *Start* Document
* Who you are / where you are → *Start* Document
* References to external databases → *Start* Document
* The Data™  → *Event* Document
* Detector calibrations, dark frames, flat fields , or masks  → *Event* Document (probably in its own stream)
* The shape / data type / units of The Data™  → *Event Descriptor* Document in the *data_keys* entry
* Anything you read from the controls system that is not device configuration  → *Event* Document
* Device configuration data  → *Event Descriptor* Document in the *configuration* entry

<!-- README only content. Anything below this line won't be included in index.md -->

See https://bluesky.github.io/event-model for more detailed documentation.
