import numpy


def verify_filled(event_page):
    '''Take an event_page document and verify that it is completley filled

    Parameters
    ----------
    event_page : event_page document
        The event page document to check

    Raise
    -----
    UnfilledData : exception
        Raised if any of the data in the event_page is unfilled, when raised it
        inlcudes a list of unfilled data objects in the exception message.

    Returns
    -------
    verified : Boolean
        To indicate if it is correctly filled, this is done make code easier to
        read (i.e. use `if verified_filled(event_page) ...`) it will never
        return False as it will raise an exception.
    '''

    if not all(map(all, event_page['filled'].values())):
        # check that all event_page data is filled.
        unfilled_data = []
        for field in event_page['filled']:
            if not event_page['filled'][field]:
                for field, filled in event_page['filled'].items():
                    if not all(filled):
                        unfilled_data.append(field)
                        # Note: As of this writing, this is a slightly
                        # aspirational error message, as event_model.Filler has
                        # not been merged yet. May need to be revisited if it
                        # is renamed or kept elsewhere in the end.
                        raise UnfilledData("unfilled data found in "
                                           "{!r}. Try passing the parameter "
                                           "`gen` through `event_model.Filler`"
                                           " first.".format(unfilled_data))
    else:
        verified = True
        return verified


def sanitize_doc(doc):
    '''Takes in an event-model document and returns a copy which has all the
    numpy objects converted to built-in pyton versions.

    This is useful for sanitzing documents prior to sending to a mongo database
    or a json file.

    Parameters
    ----------
    doc : event-model document.
        The event-model document to be sanitized

    Returns
    -------
    sanitized_doc : event-model document
        The event-model document with numpy objects converted to built-in pyton
        types.
    '''
    sanitized_doc = doc.copy()
    _apply_to_dict_recursively(doc, _sanitize_numpy)

    return sanitized_doc


def _sanitize_numpy(val):
    '''Convert any numpy objects into built-in Python types.

    Parameters
    ----------
    val : The potential numpy object to be converted.

    Returns
    -------
    val : The input parameter, converted to a built-in python type if it is a
        numpy type.
    '''
    if isinstance(val, (numpy.generic, numpy.ndarray)):
        if numpy.isscalar(val):
            return val.item()
        return val.tolist()
    return val


def _apply_to_dict_recursively(dictionary, func):
    '''
    step through a dictionary of dictionaries recursively and apply a function
    to each value.

    Parameters
    ----------
    dictionary : dict
        The dictionary of dictionaries to be recursivly searched.
    func : function
        A function to apply to each value in dictionary.
    '''

    for key, val in dictionary.items():
        if hasattr(val, 'items'):
            dictionary[key] = _apply_to_dict_recursively(val, func)
        dictionary[key] = func(val)


class UnfilledData(Exception):
    """raised when unfilled data is found"""
    pass
