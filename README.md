
<a href="http://bitly.com/2grT54q"><img src="https://cdn.codementor.io/badges/i_am_a_codementor_dark.svg" alt="I am a codementor" style="max-width:100%"/></a><a href="https://raw.githubusercontent.com/LinuxArchitects/AWS_Kinesis-/master/AWS_Kinesis.png" height="50"> 
 [![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=WX4EKLLLV49WG)

#  AWS Kinesis



Description : a python script to push event to kinesis

HOW It WORKS
================
The configuration is inside the python script : 

Requierements
================
aws_region
stream_name
record_delimiter
```
config = dict(
    aws_region='eu-west-3',
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    kinesis_concurrency=1,
    kinesis_max_retries=10,
    record_delimiter='\n',
    stream_name='recomender',
    )
```

