import pendulum
import logging

from airflow.decorators import dag, task

@dag(
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def task_dependencies():

    @task()
    def hello_world():
        logging.info("Hello World")

    @task()
    def addition(first,second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first,second):
        logging.info(f"{first -second} = {first-second}")
        return first-second

    @task()
    def division(first,second):
        logging.info(f"{first} / {second} = {int(first/second)}")   
        return int(first/second)     

# TODO: call the hello world task function
# TODO: call the addition function with some constants (numbers)
# TODO: call the subtraction function with some constants (numbers)
# TODO: call the division function with some constants (numbers)
# TODO: create the dependency graph for the first three tasks
# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task


    # hello represents a discrete invocation of hello world
    hello=hello_world()
    
    # two_plus_two represents the invocation of addition with 2 and 2
    two_plus_two=addition(2,2)
    
    # two_from_six represents the invocation of subtraction with 6 and 2
    two_from_six=subtraction(6,2)

    # eight_divided_by_two represents the invocation of division with 8 and 2
    eight_divided_by_two = division(8,2)

    # sum represents the invocation of addition with 5 and 5
    sum= addition(5,5)

    # difference represents the invocation of subtraction with 6 and 4
    difference = subtraction(6,4)

    # sum_divided_by_difference represents the invocation of division with the sum and the difference
    sum_divided_by_difference = division(sum,difference)
    
#  TODO: assign the result of the addition function to a variable
#  TODO: assign the result of the subtraction function to a variable
#  TODO: pass the result of the addition function, and the subtraction functions to the division function
# TODO: create the dependency graph for the last three tasks

 # hello to run before two_plus_two and two_from_six
    hello >> two_plus_two 
    hello >> two_from_six


    # two_plus_two to run before eight_divided_by_two
    two_plus_two >> eight_divided_by_two

    # two_from_six to run before eight_divided_by_two
    two_from_six >> eight_divided_by_two


    # Notice division waits for subtraction and addition to run

    # sum to run before sum_divided_by_difference
    sum >> sum_divided_by_difference

    # difference to run before sum_divided_by_difference
    difference >> sum_divided_by_difference

task_dependencies_dag=task_dependencies()
