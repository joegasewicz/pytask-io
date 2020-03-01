![PyPI](https://img.shields.io/pypi/v/pytask-io)
![Read the Docs](https://img.shields.io/readthedocs/pytask-io)
![GitHub](https://img.shields.io/github/license/joegasewicz/pytask_io)

# Pytask IO


Asynchronous Tasks Library using asyncio

An Asyncio based task queue that is designed to be super easy the use!

Read the docs: [Documentation](https://pytask-io.readthedocs.io/en/latest/)

![PyTask IO](assets/Group.png?raw=true "Title")

## Install
```bash
pip install pytask-io

docker run redis  # Rabbit MQ coming soon...

```


### Usage

```python
    from pytask_io import PyTaskIO
    
    # Starts the task runner
    pytask = PytaskIO(
        store_port=6379,
        store_host="localhost",
        db=0,
        workers=1,
    )
    
    # Start the PytaskIO task queue on a separate thread.
    pytask.run()
    
    # Handle a long running process, in this case a send email function
    metadata = pytask.add_task(send_email, title, body)
    
    # Try once to get the results of your email sometime in the future
    result = pytask.get_task(metadata)
    
    # Later we can use the `metadata` result to pass to `add_task`
    result = pytask.poll_for_task(metadata, tries=100, interval=60)
    
    # Stop PytaskIO completely (This will not affect any units of work that haven't yet executed)
    pytask.stop()

```
### Compatible task types
PyTaskIO will always return back the task meta data.
Do not embed Python objects of type frame, generator, traceback & context objects.
In this case you will get back the exception thrown when PyTaskIO attempts to execute the serialization.



## Authors

* **joegasewicz** - *Initial work* - [@joegasewicz](https://twitter.com/joegasewicz)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
