using JLD2
using Test
using DataFrames
using JuMP
using SDDP

function test_data_loading()

    test_data = load(joinpath(@__DIR__, "test_data", "data.jld2"), "data")
    result_data = load(joinpath(@__DIR__, "results", "data.jld2"), "data")

    joincols = ["Start date", "Year", "Month", "Day", "hour_of_year", "Area"]
    @test joincols ⊆ names(result_data)
    jdf = leftjoin(result_data, test_data, on = joincols, makeunique = true)

    for col in setdiff(names(test_data), joincols)
        @test jdf[!, "$(col)_1"] ≈ jdf[!, "$(col)"] rtol = 1e-3
        @test !(any(ismissing(result_data[!, col])))
    end

end

function test_deterministic_cost_minimization_results()

    test_data = load(
        joinpath(@__DIR__, "test_data", "deterministic_cost_minimization_results.jld2"),
        "deterministic_cost_minimization_results",
    )
    result_data = load(
        joinpath(@__DIR__, "results", "deterministic_cost_minimization_results.jld2"),
        "deterministic_cost_minimization_results",
    )

    for y = 2020:2025, local_pricing in [true, false]
        @test isa(
            result_data[y, local_pricing]["extraction"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["demand"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["storage_level"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["injection_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["prices"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["injection"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["generation"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["storage_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["generation_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["flow"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["extraction_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isapprox(
            result_data[y, local_pricing]["total_cost"],
            test_data[y, local_pricing]["total_cost"],
            rtol = 1e-3,
        )
    end
end

function test_deterministic_welfare_maximization_results()

    test_data = load(
        joinpath(@__DIR__, "test_data", "deterministic_welfare_maximization_results.jld2"),
        "deterministic_welfare_maximization_results",
    )
    result_data = load(
        joinpath(@__DIR__, "results", "deterministic_welfare_maximization_results.jld2"),
        "deterministic_welfare_maximization_results",
    )

    for y = 2020:2025, local_pricing in [true, false]
        @test isa(
            result_data[y, local_pricing]["extraction"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["demand"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["storage_level"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["injection_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["prices"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["injection"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["generation"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["storage_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["generation_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["flow"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["extraction_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isapprox(
            result_data[y, local_pricing]["total_cost"],
            test_data[y, local_pricing]["total_cost"],
            rtol = 1e-3,
        )
    end

end

function test_deterministic_strategic_behavior()

    test_data = load(
        joinpath(@__DIR__, "test_data", "deterministic_strategic_behavior_results.jld2"),
        "deterministic_strategic_behavior_results",
    )
    result_data = load(
        joinpath(@__DIR__, "results", "deterministic_strategic_behavior_results.jld2"),
        "deterministic_strategic_behavior_results",
    )

    for y = 2020:2025, local_pricing in [true, false]
        @test isa(
            result_data[y, local_pricing]["extraction"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["demand"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["storage_level"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["injection_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["prices"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["injection"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["generation"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["storage_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isa(
            result_data[y, local_pricing]["generation_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[y, local_pricing]["flow"],
            JuMP.Containers.DenseAxisArray{Float64,4},
        )
        @test isa(
            result_data[y, local_pricing]["extraction_capacity"],
            JuMP.Containers.DenseAxisArray{Float64,2},
        )
        @test isapprox(
            result_data[y, local_pricing]["total_cost"],
            test_data[y, local_pricing]["total_cost"],
            rtol = 1e-3,
        )
    end
end

function test_stochastic_cost_minimization_results()

    result_data = load(
        joinpath(@__DIR__, "results", "stochastic_cost_minimization_results.jld2"),
        "stochastic_cost_minimization_results",
    )

    for (idx, y) in enumerate(2020:2025)
        @test result_data[idx][2][:noise_term] == y
        @test isa(
            result_data[idx][2][:injection],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[idx][2][:extraction],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(
            result_data[idx][2][:storage_capacity],
            JuMP.Containers.DenseAxisArray{SDDP.State{Float64},2},
        )
        @test isa(
            result_data[idx][2][:generation],
            JuMP.Containers.DenseAxisArray{Float64,3},
        )
        @test isa(result_data[idx][2][:flow], JuMP.Containers.DenseAxisArray{Float64,3})
        @test isa(result_data[idx][2][:prices], JuMP.Containers.DenseAxisArray{Float64,2})
        @test isa(
            result_data[idx][2][:extraction_capacity],
            JuMP.Containers.DenseAxisArray{SDDP.State{Float64},2},
        )
        @test isa(
            result_data[idx][2][:generation_capacity],
            JuMP.Containers.DenseAxisArray{SDDP.State{Float64},2},
        )
        @test isa(result_data[idx][2][:demand], JuMP.Containers.DenseAxisArray{Float64,2})
        @test isa(
            result_data[idx][2][:injection_capacity],
            JuMP.Containers.DenseAxisArray{SDDP.State{Float64},2},
        )

    end
end
@testset "All Tests" begin
    @testset "Data Loading" begin
        test_data_loading()
    end
    @testset "Deterministic Cost Minimization" begin
        test_deterministic_cost_minimization_results()
    end
    @testset "Deterministic Welfare Maximization" begin
        test_deterministic_welfare_maximization_results()
    end
    @testset "Deterministic Strategic Behavior" begin
        test_deterministic_strategic_behavior()
    end
    @testset "Stochastic Cost Minimization" begin
        test_stochastic_cost_minimization_results()
    end

end
