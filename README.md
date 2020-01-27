# Pytask IO

Asynchronous Tasks Library using asyncio

An Asyncio based task queue that is designed to be super easy the use!

![PyTask IO](assets/Group.png?raw=true "Title")

## Install
```bash
pip install pytask-io

docker run redis

```



### Usage

```python
    # Starts the task runner
    pytask = PytaskIO(
        store_port=8080,
        store_host="localhost",
        broker="rabbitmq"  # coming soon!
    )
    
    # Handle a long running process, in this case a send email function
    task_id = pytask.add_task(send_email, title, body)
    
    # Try once to get the results of your email sometime in the future
    result = get_task(task_id) # returns either the result or false
    
    # Or poll for the results
    result = poll_for_task_results(task_id, tries=100, interval=5, interval_type="minutes")
    
```

### Initial design
![PyTask IO](assets/design.png?raw=true "Title")
## Authors

* **joegasewicz** - *Initial work* - [@joegasewicz](https://twitter.com/joegasewicz)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)