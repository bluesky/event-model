# Event Model

This repository contains documents that specify the schema for an event-based
data model used at NSLS-II. Other projects in the organization use this model;
it is their common way of organizing event-based data.

See [this page of the NSLS-II
documentation](https://nsls-ii.github.io/architecture-overview.html) for an illustrated
overview of the model.

The documents are specified using jsonschema. See this
[excellent tutorial](http://spacetelescope.github.io/understanding-json-schema/)
for more on jsonschema.

## Conda Recipes

Install the most recent tagged build: `conda install event-model -c lightsource2-tag`

Install the most recent tagged build: `conda install event-model -c lightsource2-dev`

Find the tagged recipe [here](https://github.com/NSLS-II/lightsource2-recipes/tree/master/recipes-tag/event-model) and the dev recipe [here](https://github.com/NSLS-II/lightsource2-recipes/tree/master/recipes-dev/event-model)

## Usage

The schemas are packaged with a Python API. (In the future, they will probably
be packaged for other languages. Get in touch if you are interested in this.)
Because of its use of the new Enum type, this package requires Python 3.4+.

There are two variables in the public API, an Enum called ``DocumentNames`` and
a dictionary called ``schemas`` that is keyed on the values of
``DocumentNames``.

```python
In [1]: import event_model

In [2]: event_model.schemas[event_model.DocumentNames.event]
Out[2]:
{'additionalProperties': False,
 'description': 'Document to record a quanta of collected data',
 'properties': {'data': {'description': 'The actual measument data',
   'type': 'object'},
  'descriptor': {'description': 'UID to point back to Descriptor for this event stream',
   'type': 'string'},
  'seq_num': {'description': 'Sequence number to identify the location of this Event in the Event stream',
   'type': 'integer'},
  'time': {'description': 'The event time.  This maybe different than the timestamps on each of the data entries',
   'type': 'number'},
  'timestamps': {'description': 'The timestamps of the individual measument data',
   'type': 'object'},
  'uid': {'description': 'Globally unique identifier for this Event',
   'type': 'string'}},
 'required': ['uid', 'data', 'timestamps', 'time', 'descriptor', 'seq_num'],
 'title': 'event',
 'type': 'object'}
```

Use it in conjunction with the
[jsonschema](https://pypi.python.org/pypi/jsonschema) package to validate
documents in Python.
