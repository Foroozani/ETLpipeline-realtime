# Step Functions 

## 7 types of states
1. A `Task` state ("Type": "Task") represents a single unit of work performed by a state machine. All work in your state machine is done by [Tasks](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html). A task performs work by using an activity or an AWS Lambda function, or by passing parameters to the API actions of other services.
2. A `Pass` state ("Type": "Pass") passes its input to its output, without performing work. Pass states are useful when constructing and debugging state machines.

In addition to the common state fields, [Pass](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-pass-state.html) states allow the following fields.
