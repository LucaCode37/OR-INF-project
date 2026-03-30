# Group members:
# 496363 - Luca Noack
# 497833 - Irina Makarova
# This project solution was implemented together by both group members.

include(joinpath(@__DIR__, "src.jl"))

optimizer = Gurobi.Optimizer

combine_data()
create_data_inspection()

get_deterministic_cost_minimization_results(optimizer)
create_deterministic_cost_minimization_results_visualization()

get_deterministic_welfare_maximization_results(optimizer)
get_deterministic_strategic_behavior_results(optimizer)
create_deterministic_elastic_results_visualization()

get_stochastic_cost_minimization_results(optimizer, true, 10, 1e+10)
create_stochastic_cost_minimization_results_visualization()
