# Random jupyter notebooks

This is just a collection of some notebooks I worked on or prototyping various things hobby projects.



## Tickets

* **[issue-1166_benchmarking_clingen_evidences](issue-1166_benchmarking_clingen_evidences)**: checking score distribution of ClinGen evidence strings

* **[issue-1249_prototyping_new_json_schema](issue-1249_prototyping_new_json_schema)**: as part of the rewrite efforts the evidence input was re-structured. To formulate these changes a new schema was developed reflecting the new requirements.

* **[issue-1284-Phenodigm_rewrite](issue-1284-Phenodigm_rewrite)**: as part of the rewrite efforts the in-house maintained evidence parsers are also updated. The PhenoDigm parser is probably the most complicated one among all. The script was written in basic Python and is now migrated to PySpark. 

* **[issue-1331_networks_issue_signor](issue-1331_networks_issue_signor)**: after the October Intact release, we have identified some discrepancy in the way we show directed interactions imported from Signor. This notebook tracks this problem.

* **[issue-1330_String_data_missing](issue-1330_String_data_missing)**: in the same release, an other interaction related issue had been identified: an entire type of interactions are missing from the STRING table. Let's find out why. 


## Quick tasks

* **[2020.09.15_checking_intact_data](2020.09.15_checking_intact_data)**: the first release of the networks data dump is being checked. Just to see if everything is alright.

* **[2020.10.01_EVA_scoring](2020.10.01_EVA_scoring)**: during the summer the EVA sourced genetic evidence was re-structured, new values were added. In this notebooks I inverstigated if these new values can be used to improve scoring to provide a better drug-target prioritization.

* **[2020.10.06_testing_new_networks_data_release](2020.10.06_testing_new_networks_data_release)**: to make sure the previously found issues are fixed an other round of checks were applied.

* **[2021.02.15_release_check](2021.02.15_release_check)**: the 21.02 release introduced a moderate number of new ClinGen and Gene2Phenotype evidence. The aim of this task was to find out if any of these new evidence mean a totally new association not yet supported by any other genetic evidence.

* **[TEP_parsing](TEP_parsing)**: prototyping a pipeline to retrieve TEP (target enabling package) from [SGC](https://www.thesgc.org/).


## Random little projects

* **[2020.12.30_visual_tests](2020.12.30_visual_tests)**: this exercise helped to develop new ways to visualize the number of different projects, their scales over the years.

* **[HMRC_scambaiting](HMRC_scambaiting)**: This is just a small script to automate fooding scammers' database with fake data. This was the time I learnt about the [faker](https://faker.readthedocs.io/) library. That is just an amazing tool.


