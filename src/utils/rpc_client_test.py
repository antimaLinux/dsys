from __future__ import print_function, unicode_literals
# client service

import time
try:
    import xmlrpclib
except ImportError:
    import xmlrpc as xmlrpclib

server = xmlrpclib.Server('http://localhost:8000')
print(server.chop_in_half('I am a confidant guy'))
print(server.repeat('Repetition is the key to learning!\n', 5))
print(server._string('<= underscore'))
#print(server.python_string.join(['I', 'like it!']))
print(server.all_strings())  # Will throw an exception)
print(server.all_strings_json())
