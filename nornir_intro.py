#!/usr/bin/env python
# coding: utf-8
"""
NORNIR BASICS

Working through the initialization from Nornir documentation
https://nornir.readthedocs.io/en/latest/tutorial/index.html
"""
from nornir import InitNornir               # initializing Nornir
from nornir.core.inventory import Host      # inventory handling
from nornir.core.filter import F            # F object for advanced filtering
from nornir.core.task import Task, Result, AggregatedResult, MultiResult
from nornir.core.exceptions import NornirExecutionError
from nornir_utils.plugins.functions import print_result
import logging                              # to log errors
import json                                 # display inventory
import pprint                               # display inventory
from typing import Dict                     # to annotate code with types


if __name__ == "__main__":
    #  1 Initializing Nornir

    # 1.1 Simple Nornir - Object creation with all files necessary in place
    nr = InitNornir(config_file="config.yaml")

    #  1.2 Initialize nornir programmatically without a configuration file
    nr1 = InitNornir(
        runner={
            "plugin": "threaded",
            "options": {
                "num_workers": 100,
            },
        },
        inventory={
            "plugin": "SimpleInventory",
            "options": {
                "host_file": "inventory/hosts.yaml",
                "group_file": "inventory/groups.yaml"
            },
        },
    )

    # 1.3 Combination of both methods, e.g. to overwrite config.yaml value
    nr2 = InitNornir(
        config_file="config.yaml",
        runner={
            "plugin": "threaded",
            "options": {
                "num_workers": 50,     # in this config.yaml "num_workers": 100
            },
        },
    )
    print(f'Initialized with new dictionary, threads changed to: {nr2.config.runner.options["num_workers"]}')

    # *****************************************
    #  2 Inventory
    print(Host.schema())
    print(json.dumps(Host.schema(), indent=4))    # json object, easier to read

    print("Hosts:")
    pprint.pprint(nr.inventory.hosts, indent=1)
    print("-----\nGroups:")
    pprint.pprint(nr.inventory.groups)
    # or tap in with assigning dictionary to variable
    host = nr.inventory.hosts["leaf01.bma"]
    for k, v in host.items():
        print(f"{k}: {v}")

    # *****************************************
    #  3 Inheritance
    leaf01_bma = nr.inventory.hosts["leaf01.bma"]
    print(leaf01_bma["domain"])     # comes from the group `global` and has therefore this "domain" value
    print(leaf01_bma["asn"])        # comes from group `eu` and has therefore this "asn" value

    # If neither the host nor the parents have a specific value for a key, values in defaults will be returned.
    leaf01_cmh = nr.inventory.hosts["leaf01.cmh"]
    print(leaf01_cmh["domain"])     # comes from defaults

    # python throws normal error, if the key is not existing
    # leaf01_cmh["wrong_key"] # causes crah if uncommented

    # can be caught as usual:
    try:
        leaf01_cmh["wrong_key"]
    except KeyError as e:
        print(f"Couldn't find key: {e}")

    # also when using .data the key won't be found
    print(f"leaf01.cmh keys: {leaf01_cmh.data}")
    try:
        leaf01_cmh.data["domain"]
    except KeyError as e:
        print(f"Couldn't find key: {e}")

    # *****************************************
    # 4 Filtering
    # 4.1 Basic filtering by Key-Value pairs

    # One Pair
    print(nr.filter(site="cmh").inventory.hosts.keys())

    # Multiple Pairs
    print(nr.filter(site="cmh", role="spine").inventory.hosts.keys())

    # Culmulative Method
    print(nr.filter(site="cmh").filter(role="spine").inventory.hosts.keys())

    # Create new variable ...
    cmh = nr.filter(site="cmh")
    print(cmh.inventory.hosts.keys())
    # ... and filter by properties
    print(cmh.filter(role="spine").inventory.hosts.keys())
    print(cmh.filter(role="leaf").inventory.hosts.keys())

    # Children of group - returned as a set
    print(nr.inventory.children_of_group("eu"))
    print(type(nr.inventory.children_of_group("eu")))

    # ---
    # 4.2 Advanced Filtering
    # 4.2.1 Filter Functions - simple queries (in this case simply filter by length of name)
    def has_long_name(this_host):
        """returns True or False if length of host == 11"""
        return len(this_host.name) == 11
    print(nr.filter(filter_func=has_long_name).inventory.hosts.keys())
    # or via lambda function
    print(nr.filter(filter_func=lambda x: len(x.name) == 11).inventory.hosts.keys())

    # 4.2.2 Filter Object - complex queries
    # hosts in group cmh
    cmh = nr.filter(F(groups__contains="cmh"))  # first create the F object
    print(cmh.inventory.hosts.keys())
    # 4.2.2.1 Logic filtering
    linux_or_eos = nr.filter(F(platform="linux") | F(platform="eos"))            # devices running either linux or eos
    print(linux_or_eos.inventory.hosts.keys())
    cmh_and_not_spine = nr.filter(F(groups__contains="cmh") & ~F(role="spine"))  # cmh devices that are not spines
    print(cmh_and_not_spine.inventory.hosts.keys())
    # 4.2.2.2 Access nested Data
    nested_string_asd = nr.filter(F(nested_data__a_string__contains="asd"))
    print(nested_string_asd.inventory.hosts.keys())
    a_dict_element_equals = nr.filter(F(nested_data__a_dict__c=3))
    print(a_dict_element_equals.inventory.hosts.keys())
    a_list_contains = nr.filter(F(nested_data__a_list__contains=2))
    print(a_list_contains.inventory.hosts.keys())

    # *****************************************
    # 5 Tasks (function taking Task as first paramater and returning Result)
    nr = InitNornir(config_file="config.yaml")
    nr = nr.filter(site="cmh", role="host")     # filtering objects (to simplify output)
    # 5.1 Simple function

    def hello_world(task: Task) -> Result:
        """Return string with host name and hardcoded message"""
        return Result(
            host=task.host,
            result=f"{task.host.name} says hello world!"
        )
    result = nr.run(task=hello_world)  # To execute a task you can use the run method:
    print_result(result)
    # 5.2 Simple function with additional parameters

    def say(task: Task, text: str = "default message") -> Result:
        """Return string with host name and dynamic message, default message if nothing is given"""
        return Result(
            host=task.host,
            result=f"{task.host.name} says {text}"
        )
    result = nr.run(
        name="Saying goodbye in a very friendly manner",  # "rename" the function, if not given, function name is used
        task=say,
        text="buhbye!"  # additional parameter
    )
    print_result(result)

    # 5.3 Grouping tasks - more complex functionality by combining smaller building blocks (tasks calling other tasks).
    # another "small" task
    def count(task: Task, number: int) -> Result:
        """takes in a number and returns an f-string with list counting number elements"""
        return Result(
            host=task.host,
            result=f"{[n for n in range(0, number)]}"
        )

    # combine tasks above to a grouped task
    def greet_and_count(task: Task, number: int = 1,
                        greet: str = "default greet", bye: str = "default bye") -> Result:
        """uses the say and count functions, grouping the tasks with parameters for number, greet and bye
           (default: 1, "default greet" and "default bye" """
        task.run(  # call the say function
            name="The say function is called with the greet parameter",
            task=say,
            text=greet,
        )
        task.run(
            name="The count function is called ",
            task=count,
            number=number,
        )
        task.run(
            name="The say function is called with the bye parameter",
            task=say,
            text=bye,
        )
        # the task (function) can have more code within itself, e.g. checking even or odd
        even_or_odds = "even" if (number + 1) % 2 == 1 else "odd"
        return Result(
            host=task.host,
            result=f"{task.host} counted {even_or_odds} times!",
        )

    this_num = 5
    this_greet = "Hello, there"
    this_bye = "Bye now"
    result = nr.run(
        name=f"Counting to {this_num} and using the say function to greet and say bye",
        task=greet_and_count,
        number=this_num,
        greet=this_greet,
        bye=this_bye
    )
    print_result(result)

    # *****************************************
    # 6 Processing Results
    def say_new(task: Task, text: str = "default msg", exception_host_name: str = "host2.cmh") -> Result:
        """Return string with host name and message, if no message is given 'default msg' is default,
           to if the hostname equals exception_host_name, an error is raised."""
        if task.host.name == exception_host_name:
            raise Exception(f"An Exception was raised on host {exception_host_name}")
        return Result(
            host=task.host,
            result=f"{task.host.name} says {text}"
        )

    def greet_and_count_new(task: Task, number: int = 1,        # adapt to say_new and add severity logging
                            greet: str = "default greet", bye: str = "default bye") -> Result:
        """uses the say and count functions, grouping the tasks with parameters for number, greet and bye
           (default: 1, "default greet" and "default bye" """
        task.run(  # call the say function
            name="The say function is called with the greet parameter",
            severity_level=logging.DEBUG,
            task=say_new,
            text=greet,
        )
        task.run(
            name="The count function is called ",
            task=count,
            number=number,
        )
        task.run(
            name="The say function is called with the bye parameter",
            severity_level=logging.DEBUG,
            task=say_new,
            text=bye,
        )
        even_or_odds = "even" if (number+1) % 2 == 1 else "odd"
        return Result(
            host=task.host,
            result=f"{task.host} counted {even_or_odds} times!",
        )
    # re-instantiate the nr object and filter to cmh
    nr = InitNornir(config_file="config.yaml")
    cmh = nr.filter(site="cmh", type="host")

    # 6.1 Simple approach
    this_num = 5
    this_greet = "Hello, there"
    this_bye = "Bye now"
    result = cmh.run(
        name=f"Counting to {this_num} and using the say function to greet and say bye",
        task=greet_and_count_new,
        number=this_num,
        greet=this_greet,
        bye=this_bye
    )
    print_result(result)  # not all tasks are printed, by default ONLY the info level info will be printed
    # A failed task will always have its severity level changed to ERROR regardless of the one specified by the user.
    # With the exception_host_name parameter an error was raised, so it will **NOT** be printed, if not specified.
    print("\n---\nNo severity_level specification, only the INFO level is printed:")
    print_result(result["host1.cmh"])
    print("\n---\nWith severity_level=logging.DEBUG, all results are printed: ")
    print_result(result, severity_level=logging.DEBUG)

    # 6.2 Programmatic approach- the task groups will return dict-like object AggregatedResult (to access host directly)
    print("\n---\nAggregatedResult:")
    print(result)
    print("\n---\nAccess by using the keys:")
    print(f"Keys: {result.keys()}")
    print(f"Specify key: {result['host1.cmh']}")
    # Each AggregatedResult contains a list-like MultiResult object, therefore can be indexed.
    print(f"Specify key and indexed MultiObject: {result['host1.cmh'][0]}")
    # Each result also contains the changed and failed from the respective host. Therefore, return the directly:
    for host in ["host1.cmh", "host2.cmh"]:
        print(f"{host} -> changed: {result[host].changed}, failed: {result[host].failed}")

    # *****************************************
    # 7 Failed Tasks
    # 7.1 Basics - the .failed property will be set True when a task failed, a dict-like .failed_hosts obj is created
    print(f"An error was raised: {result.failed}")
    print(f"Failed hosts stored in .failed_hosts: {result.failed_hosts}")
    print(f"result['host2.cmh'].exception")
    print(f"result['host2.cmh'][1].exception")
    # NornirExecutionError: built-in method raising an exception if the task had an error
    try:
        result.raise_on_error()
    except NornirExecutionError:
        print("An error was raised.")

    # 7.2 Skipped hosts - when re-instating cmh, there was an error raised on host2.cmh
    print(nr.data.failed_hosts)  # the set failed_hosts keeping track in shared data object nr.data.failed_hosts
    #                            # failed hosts will be tracked and future tasks WILL NOT be run on them by Nornir.
    # New task

    def new_task(task: Task) -> Result:
        """similar to say, to show failed tasks handling"""
        return Result(host=task.host, result=f"{task.host.name}: new task was run on.")
    # run new_task (incl. the "flagged" host2.cmh
    result = cmh.run(task=new_task)
    print("\n---\nhost2.cmh was flagged as failed hosts, therefore new_task was not run on them:")
    print_result(result)
    # to any failed hosts, set parameter on_failed to True when calling the .run function (default is False):
    print("\n---\nhost2.cmh was flagged as failed hosts, but is included as on_failed=True:")
    result = cmh.run(task=new_task, on_failed=True)
    print_result(result)
    # to exclude all "good" hosts, set parameter on_good to False when calling the .run function (default is True):
    print("\n---\nhost1.cmh was not flagged as failed hosts, but is excluded as on_good=False:")
    result = cmh.run(task=new_task, on_failed=True, on_good=False)
    print_result(result)
    # 7.3 Resetting .failed_hosts - make flagged hosts eligible for future tasks again by resetting the list completely
    nr.data.reset_failed_hosts()
    print(f"\n---\nAfter using .reset_failed_hosts() method: failed_hosts is empty: {nr.data.failed_hosts}")
    # ToDo: (lookup recover_host for individually resetting hosts)

    # *****************************************
    # 8 Processors - Alternative way of dealing with the results of a task


    class PrintResult:
        def task_started(self, task: Task) -> None:
            print(f">>> starting: {task.name}")

        def task_completed(self, task: Task, result: AggregatedResult) -> None:
            print(f">>> completed: {task.name}")

        def task_instance_started(self, task: Task, host: Host) -> None:
            pass

        def task_instance_completed(
                self, task: Task, host: Host, result: MultiResult
        ) -> None:
            print(f"  - {host.name}: - {result.result}")

        def subtask_instance_started(self, task: Task, host: Host) -> None:
            pass  # to keep example short and sweet we ignore subtasks

        def subtask_instance_completed(
                self, task: Task, host: Host, result: MultiResult
        ) -> None:
            pass  # to keep example short subtasks are ignored # todo look into subtasks


    class SaveResultToDict:
        def __init__(self, data: Dict[str, None]) -> None:
            self.data = data

        def task_started(self, task: Task) -> None:
            self.data[task.name] = {}
            self.data[task.name]["started"] = True

        def task_completed(self, task: Task, result: AggregatedResult) -> None:
            self.data[task.name]["completed"] = True

        def task_instance_started(self, task: Task, host: Host) -> None:
            self.data[task.name][host.name] = {"started": True}

        def task_instance_completed(
                self, task: Task, host: Host, result: MultiResult
        ) -> None:
            self.data[task.name][host.name] = {
                "completed": True,
                "result": result.result,
            }

        def subtask_instance_started(self, task: Task, host: Host) -> None:
            pass  # to keep example short and sweet we ignore subtasks

        def subtask_instance_completed(
                self, task: Task, host: Host, result: MultiResult
        ) -> None:
            pass  # to keep example short subtasks are ignored # todo look into subtasks


    def greeter(task: Task, greet: str) -> Result:
        """simple function like say, s.a."""
        return Result(host=task.host, result=f"{greet}! my name is {task.host.name}")

    data = {}  # this is the dictionary where SaveResultToDict will store the information

    # similary to .filter, with_processors returns a copy of the nornir object but with
    # the processors assigned to it. Let's now use the method to assign both processors
    nr_with_processors = nr.with_processors([SaveResultToDict(data), PrintResult()])

    # now we can use nr_with_processors to execute our greeter task
    nr_with_processors.run(
        name="hi!",
        task=greeter,
        greet="hi",
    )
    nr_with_processors.run(
        name="bye!",
        task=greeter,
        greet="bye",
    )

    print(json.dumps(data, indent=4))
