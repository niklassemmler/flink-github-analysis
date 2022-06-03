# Github Analaysis on Flink

## Collect data

- Use github_access to collect data

## Data schemes

## Ideas

- Can we create a metric to show how friendly repos are to new contributors?
- Should we discern between Ververica employees and non-employees?
- Would it be enough to just classify the low-volume contributors? How would we do this?

- Correlate component and likelihood of receiving a review
- Correlate component and life time of PR


### Possible KPIs

- Time to first review
- Average life time of a PR
- Number of open PRs


### Fundamental question

- Do first-time contributors that receive a timely review come have a higher chance to come back?

- [X] Counts of PRs that did/didn't receive at least one review
- [X] Number of reviews per day
- [ ] Time between output and review (avg)
- [X] Time to first review
- [X] Distribution of PR age

New questions:

- [ ] How long are closed PRs open?
- [ ] How are reviews distributed over people? Did this change over time?
- [ ] User base: How many contributors come from Compare Alibaba and Ververica vs external?
  - Can we get a list of all previous Ververica employees?
  - Do Alibaba employees use alibaba mails?
- [X] How many PRs have a FLINK id attached
- [ ] Since 14.03.2019 PRs are labelled by component. Has this changed the number of reviews per component? (Compare PRs created before and PRs created since. The labelling was attached retroactively.)
- [ ] Besides labels, do we want to identify commit by module (i.e. via files)?

Operational tasks

- [X] Get commit data (via ref)
- [ ] Get review data (via prs)
- ~[ ] Get contributor data (via repository)~
- [ ] Get PR-label data (via pr)
- [ ] Get PR-file data (via pr)
- [ ] create traversal for multi-level pagination 
- [ ] Use pandas.json_normalize for normalization

Two-level data extraction 
- Manage multiple cursors simultaneously
