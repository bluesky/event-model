.. Packaging Scientific Python documentation master file, created by
   sphinx-quickstart on Thu Jun 28 12:35:56 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

===================================
 Bluesky Event Model Documentation
===================================

A primary design goal of bluesky is to enable better research by recording
rich metadata alongside measured data for use in later analysis. Documents are
how we do this.


This repository contains the formal schemas for bluesky's streaming data model
and some Python tooling for composing, validating, and transforming documents
in the model.



Where is my data?
=================

For the full details and schema please see the :ref:`data_model` section.  This is a very quick guide to where
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



.. toctree::
   :maxdepth: 2

   installation
   data-model
   api
   external
   use-cases
   release-history
