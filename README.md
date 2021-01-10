## Shortest job first

The work was done according to the educational option twelfth task

Discipline: parallel programming

Python ver: 3.5 and higher

Shortest job next (SJN), also known as shortest job first (SJF)
## Description

The input.txt file contains next parametrs.
1. Type of algorithm SJF non-preemptive or SJF preemptive
1. Quant of time or QT
1. Maximal burst time
1. Maximal priority
1. Count of resources (professors)
Then comes the attributes of the professors (Name, Discipline, Total count students per time) This params repeated as much as specified in the parameter "Count of resources"
1. "Total count of threads" also as know "count of students"
Then comes the streams attributes (students), (Name, Group, Discipline which a student must pass, Burst time, Priority, Arrival time) This params repeadted as much specified in the parametr "Count of students"

In this project professor - resource. Student - thread (stream).
If a student needs a resource that the professor has and he available, then a student will be work with this professor. 

Depending on the regime (preemptive) or (non-preemptive), the student's work with the professor may be interrupted if the arriving student has a “burst time” or “priority” that is more preferable than has the current student.

## Output files

1. [Output file for preemptive SJF](output/output_preemptive.txt)
1. [Output file for non-preemptive SJF](output/output_nonpreemptive.txt)

## License

See the [LICENSE](LICENSE) file for license rights and limitations (MIT).

