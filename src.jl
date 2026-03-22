using CSV
using DataFrames
using Dates
using Gurobi
using JLD2
using JuMP
using Makie, GLMakie, CairoMakie
using SDDP
using Statistics

function modify_dates(df::DataFrame)
    @assert issorted(df[!, "Start date"])

    df.Year .= year.(df[!, "Start date"])
    df.Month .= month.(df[!, "Start date"])
    df.Day .= day.(df[!, "Start date"])
    filter!([:Month, :Day] => (m, d) -> !(m == 2 && d == 29), df)

    df = combine(groupby(df, :Year)) do g
        @assert nrow(g) == 8760
        g.hour_of_year = 1:8760
        return g
    end
end

function read_generation_data(area::String)
    df = CSV.read(
        joinpath(
            @__DIR__,
            "Data",
            "Actual_generation_202001010000_202601010000_Hour_$(area).csv",
        ),
        DataFrame;
        delim = ";",
        groupmark = ',',
        decimal = '.',
        dateformat = dateformat"u d, yyyy HH:MM p",
        types = Dict(
            "Start date" => DateTime,
            "End date" => DateTime,
        ),
        missingstring = "-",
        select = [
            "Start date",
            "Wind offshore [MWh] Calculated resolutions",
            "Wind onshore [MWh] Calculated resolutions",
            "Photovoltaics [MWh] Calculated resolutions",
        ],
    )
    rename!(n -> Symbol(replace(String(n), r"\s+Calculated resolutions$" => "")), df)
    if "Wind offshore [MWh]" ∉ names(df)
        df[!, "Wind offshore [MWh]"] .= 0.0
    end
    df[!, :Area] .= area

    foreach(c -> replace!(c, missing => 0), eachcol(df))
    disallowmissing!(df)

    modify_dates(df)

    @assert issorted(df[!, "Start date"])

    return df
end

function read_generation_data(areas::Vector{String})
    dfs = vcat((read_generation_data(area) for area in areas)...)
    return dfs
end

function read_demand_data(area::String)
    df = CSV.read(
        joinpath(
            @__DIR__,
            "Data",
            "Actual_consumption_202001010000_202601010000_Hour_$(area).csv",
        ),
        DataFrame;
        delim = ";",
        groupmark = ',',
        decimal = '.',
        dateformat = dateformat"u d, yyyy HH:MM p",
        types = Dict(
            "Start date" => DateTime,
            "End date" => DateTime,        ),
        select = ["Start date", "grid load [MWh] Calculated resolutions"],
    )
    rename!(n -> Symbol(replace(String(n), r"\s+Calculated resolutions$" => "")), df)
    df[!, :Area] .= area
    foreach(c -> replace!(c, missing => 0), eachcol(df))
    disallowmissing!(df)
    modify_dates(df)
    return df
end

function read_demand_data(areas::Vector{String})
    dfs = vcat((read_demand_data(area) for area in areas)...)
    return dfs
end

function read_price_data()
    df = CSV.read(
        joinpath(@__DIR__, "Data", "Day-ahead_prices_202001010000_202601010000_Hour.csv"),
        DataFrame;
        delim = ";",
        groupmark = ',',
        decimal = '.',
        dateformat = dateformat"u d, yyyy HH:MM p",
        types = Dict(
            "Start date" => DateTime,
            "End date" => DateTime,
            "Germany/Luxembourg [€/MWh] Calculated resolutions" => Float64,
        ),
        select = ["Start date", "Germany/Luxembourg [€/MWh] Calculated resolutions"],
    )
    rename!(n -> Symbol(replace(String(n), r"\s+Calculated resolutions$" => "")), df)
    foreach(c -> replace!(c, missing => 0), eachcol(df))
    disallowmissing!(df)

    modify_dates(df)
    return df
end

function read_capacity_data(area::String)
    df = CSV.read(
        joinpath(
            @__DIR__,
            "Data",
            "Installed_generation_capacity_202001010000_202601010000_Year_$(area).csv",
        ),
        DataFrame;
        delim = ";",
        groupmark = ',',
        decimal = '.',
        dateformat = dateformat"u d, yyyy HH:MM p",
        types = Dict(
            "Start date" => DateTime,
            "End date" => DateTime,
            # "Wind offshore [MW] Original resolutions" => Float64,
            # "Wind onshore [MW] Original resolutions" => Float64,
            # "Photovoltaics [MW] Original resolutions" => Float64,
        ),
        select = [
            "Start date",
            "Wind offshore [MW] Original resolutions",
            "Wind onshore [MW] Original resolutions",
            "Photovoltaics [MW] Original resolutions",
        ],
    )

    rename!(n -> Symbol(replace(String(n), r"\s+Original resolutions$" => "")), df)
    if "Wind offshore [MW]" ∉ names(df)
        df[!, "Wind offshore [MW]"] .= 0.0
    end
    foreach(c -> replace!(c, missing => 0), eachcol(df))
    disallowmissing!(df)
    df[!, :Area] .= area
    df.Year .= year.(df[!, "Start date"])

    return df
end

function read_capacity_data(areas::Vector{String})
    dfs = vcat((read_capacity_data(area) for area in areas)...)
    return dfs
end

function calculate_availabilities(df)
    for source in ["Wind offshore", "Wind onshore", "Photovoltaics"]
        df[!, "$(source) availability"] =
            df[!, "$(source) [MWh]"] ./ df[!, "$(source) [MW]"]
        replace!(df[!, "$(source) availability"], NaN => 0.0)
    end
end

function generate_inverse_demand_parameters(data; price_elasticity = -0.2)
    data.a =
        replace(v -> v <= 0 ? 1 : v, data[!, "Germany/Luxembourg [€/MWh]"]) .*
        (1-1/price_elasticity)
    data.b =
        replace(v -> v <= 0 ? 1 : v, data[!, "Germany/Luxembourg [€/MWh]"]) ./
        (price_elasticity .* data[!, "grid load [MWh]"])
    return nothing
end

function combine_data()
    generation_data = read_generation_data(["50Hertz", "Amprion", "TenneT", "TransnetBW"])
    demand_data = read_demand_data(["50Hertz", "Amprion", "TenneT", "TransnetBW"])
    price_data = read_price_data()
    capacity_data = read_capacity_data(["50Hertz", "Amprion", "TenneT", "TransnetBW"])
    data = leftjoin(
        generation_data,
        demand_data,
        on = ["Start date", :Month, :Day, :Area, :Year, :hour_of_year],
    )
    leftjoin!(data, price_data, on = ["Start date", :Month, :Day, :Year, :hour_of_year])
    leftjoin!(data, capacity_data[!, Not(["Start date"])], on = [:Area, :Year])
    select!(
        data,
        [
            "Start date",
            "Year",
            "Month",
            "Day",
            "hour_of_year",
            "Area",
            "grid load [MWh]",
            "Wind offshore [MWh]",
            "Wind onshore [MWh]",
            "Photovoltaics [MWh]",
            "Germany/Luxembourg [€/MWh]",
            "Wind offshore [MW]",
            "Wind onshore [MW]",
            "Photovoltaics [MW]",
        ],
    )
    calculate_availabilities(data)
    generate_inverse_demand_parameters(data)
    @save joinpath(@__DIR__, "results", "data.jld2") data
    return nothing
end

function create_data_inspection(;
    plotcols = [
        "Wind offshore availability",
        "Wind onshore availability",
        "Photovoltaics availability",
    ],
    areas = ["50Hertz", "Amprion", "TenneT", "TransnetBW"],
    plotgrouping = [:Year, :Month, :Area],
)

    @load joinpath(@__DIR__, "results", "data.jld2") data
    years = sort!(unique(data[!, :Year]))
    months = sort!(unique(data[!, :Month]))

    group_map = Dict{Tuple{Int,Int,String},DataFrame}()
    for (k, g) in pairs(groupby(data, plotgrouping))
        @assert issorted(g[!, "Start date"])
        group_map[k...] = g
    end

    fig = Figure(size = (1100, 700))
    axs = [Axis(fig[i+1, 1:3], title = v) for (i, v) in enumerate(plotcols)]

    dd_year = Menu(fig[1, 1], options = years)
    dd_month = Menu(fig[1, 2], options = months)
    dd_area = Menu(fig[1, 3], options = areas)

    subdf = @lift begin
        y = years[$(dd_year.i_selected)]
        m = months[$(dd_month.i_selected)]
        a = areas[$(dd_area.i_selected)]
        group_map[(y, m, a)]
    end

    x = @lift($subdf[!, "hour_of_year"])
    ys = [@lift($subdf[!, v]) for v in plotcols]

    ls = [lines!(axs[i], x, ys[i]) for i in eachindex(plotcols)]

    on(subdf) do _
        for ax in axs
            autolimits!(ax)
            ylims!(ax, 0, 1)
        end
    end

    display(fig)

end


# PT.2.3 – Cost Optimization Model
# --------------------------------

function get_deterministic_cost_minimization_results(optimizer)
    @load joinpath(@__DIR__, "results", "data.jld2") data

    # Index sets
    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Ts = 1:8760
    previous = Dict(t => t == 1 ? 8760 : t - 1 for t in Ts)

    # Cost parameters
    discount_rate = 0.05
    lifetime_gen = Dict("Wind offshore" => 30, "Wind onshore" => 30,
                        "Photovoltaics" => 30, "Loss of Load" => 30)
    lifetime_storage = Dict("Battery" => 15, "Hydrogen" => 30)

    annuity(r, n) = r / (1 - (1 + r)^(-n))

    capex_gen = Dict("Wind offshore" => 2.8e6, "Wind onshore" => 1.2e6,
                     "Photovoltaics" => 0.6e6, "Loss of Load" => 1.0)
    mc_gen = Dict("Wind offshore" => 1.0, "Wind onshore" => 1.0,
                  "Photovoltaics" => 1.0, "Loss of Load" => 1000.0)

    capex_storage_energy = Dict("Battery" => 3e5, "Hydrogen" => 3e3)
    capex_injection = Dict("Battery" => 1.0, "Hydrogen" => 1.4e6)
    capex_extraction = Dict("Battery" => 1.0, "Hydrogen" => 6e5)
    η_inj = Dict("Battery" => 0.95, "Hydrogen" => 0.60)
    η_ext = Dict("Battery" => 0.95, "Hydrogen" => 0.60)

    flow_cost = 1.0

    # Annuity factors
    af_gen = Dict(g => annuity(discount_rate, lifetime_gen[g]) for g in Gs)
    af_storage = Dict(s => annuity(discount_rate, lifetime_storage[s]) for s in Ss)

    # Build availability and demand lookups per year
    years = 2020:2025

    deterministic_cost_minimization_results = JuMP.Containers.DenseAxisArray(
        Array{Any}(undef, length(years), 2), collect(years), [true, false]
    )

    for year in years
        year_data = filter(row -> row.Year == year, data)

        # Availability: for each (g, z, t)
        avail = Dict{Tuple{String,String,Int},Float64}()
        for row in eachrow(year_data)
            z = row.Area
            t = row.hour_of_year
            for g in ["Wind offshore", "Wind onshore", "Photovoltaics"]
                avail[(g, z, t)] = row["$(g) availability"]
            end
            avail[("Loss of Load", z, t)] = 1.0
        end

        # Demand: for each (z, t)
        demand_val = Dict{Tuple{String,Int},Float64}()
        for row in eachrow(year_data)
            demand_val[(row.Area, row.hour_of_year)] = row["grid load [MWh]"]
        end

        for local_pricing in [true, false]
            model = Model(optimizer)
            set_silent(model)

            # Variables
            @variable(model, generation_capacity[g in Gs, z in Zs] >= 0)
            @variable(model, storage_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, injection_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, extraction_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, generation[g in Gs, z in Zs, t in Ts] >= 0)
            @variable(model, injection[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, extraction[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, storage_level[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, flow[z1 in Zs, z2 in Zs, t in Ts] >= 0)

            # Objective
            @objective(model, Min,
                sum(af_gen[g] * capex_gen[g] * generation_capacity[g, z]
                    for g in Gs, z in Zs) +
                sum(af_storage[s] * capex_storage_energy[s] * storage_capacity[s, z]
                    for s in Ss, z in Zs) +
                sum(af_storage[s] * capex_injection[s] * injection_capacity[s, z]
                    for s in Ss, z in Zs) +
                sum(af_storage[s] * capex_extraction[s] * extraction_capacity[s, z]
                    for s in Ss, z in Zs) +
                sum(mc_gen[g] * generation[g, z, t]
                    for g in Gs, z in Zs, t in Ts) +
                sum(flow_cost * flow[z1, z2, t]
                    for z1 in Zs, z2 in Zs, t in Ts if z1 != z2)
            )

            # Constraints
            # 1. Generation availability
            @constraint(model, gen_avail[g in Gs, z in Zs, t in Ts],
                generation[g, z, t] <= avail[(g, z, t)] * generation_capacity[g, z])

            # 2. Injection power limit
            @constraint(model, inj_limit[s in Ss, z in Zs, t in Ts],
                injection[s, z, t] <= injection_capacity[s, z])

            # 3. Extraction power limit
            @constraint(model, ext_limit[s in Ss, z in Zs, t in Ts],
                extraction[s, z, t] <= extraction_capacity[s, z])

            # 4. Storage energy capacity limit
            @constraint(model, stor_limit[s in Ss, z in Zs, t in Ts],
                storage_level[s, z, t] <= storage_capacity[s, z])

            # 5. Storage dynamics
            @constraint(model, stor_dyn[s in Ss, z in Zs, t in Ts],
                storage_level[s, z, t] == storage_level[s, z, previous[t]] +
                    injection[s, z, t] - extraction[s, z, t])

            # 6. Market clearing (zonal energy balance)
            @constraint(model, market_clearing[z in Zs, t in Ts],
                sum(generation[g, z, t] for g in Gs) +
                sum(extraction[s, z, t] * η_ext[s] for s in Ss) +
                sum(flow[z2, z, t] for z2 in Zs if z2 != z)
                >=
                demand_val[(z, t)] +
                sum(injection[s, z, t] / η_inj[s] for s in Ss) +
                sum(flow[z, z2, t] for z2 in Zs if z2 != z)
            )

            # 7. Flow constraints
            if local_pricing
                @constraint(model, no_flow[z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[z1, z2, t] == 0)
            else
                @constraint(model, flow_cap[z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[z1, z2, t] <= 100_000)
            end

            # No self-flow
            @constraint(model, no_self_flow[z in Zs, t in Ts],
                flow[z, z, t] == 0)

            optimize!(model)
            @assert JuMP.termination_status(model) == MOI.OPTIMAL

            # Extract results
            prices = JuMP.Containers.DenseAxisArray(
                [dual(market_clearing[z, t]) for z in Zs, t in Ts],
                Zs, collect(Ts)
            )

            demand_array = JuMP.Containers.DenseAxisArray(
                [demand_val[(z, t)] for z in Zs, t in Ts],
                Zs, collect(Ts)
            )

            deterministic_cost_minimization_results[year, local_pricing] = Dict(
                "extraction"          => value.(extraction),
                "demand"              => demand_array,
                "storage_level"       => value.(storage_level),
                "injection_capacity"  => value.(injection_capacity),
                "prices"              => prices,
                "injection"           => value.(injection),
                "generation"          => value.(generation),
                "storage_capacity"    => value.(storage_capacity),
                "generation_capacity" => value.(generation_capacity),
                "total_cost"          => objective_value(model),
                "flow"                => value.(flow),
                "extraction_capacity" => value.(extraction_capacity),
            )
        end
    end

    @save joinpath(@__DIR__, "results", "deterministic_cost_minimization_results.jld2") deterministic_cost_minimization_results
    return nothing
end


# PT.2.4 – Visualization of Cost Optimal Solution
#------------------------------------------------

function create_deterministic_cost_minimization_results_visualization()
    results_path = joinpath(@__DIR__, "results", "deterministic_cost_minimization_results.jld2")
    test_path = joinpath(@__DIR__, "test_data", "deterministic_cost_minimization_results.jld2")
    path = isfile(results_path) ? results_path : test_path
    results = load(path, "deterministic_cost_minimization_results")

    cmap = Dict(
        "Storage Injection - Battery"    => "#ae393f",
        "Storage Injection - Hydrogen"   => "#0d47a1",
        "Storage Extraction - Battery"   => "#ae393f",
        "Storage Extraction - Hydrogen"  => "#0d47a1",
        "Imports"                        => "#4d3e35",
        "Exports"                        => "#754937",
        "Generation - Loss of Load"      => "#e54213",
        "Generation - Wind offshore"     => "#215968",
        "Generation - Wind onshore"      => "#518696",
        "Generation - Photovoltaics"     => "#ffeb3b",
    )

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Ts = 1:8760
    years = 2020:2025
    transport_options = [true, false]
    transport_labels = ["Local Pricing", "Perfect Competition"]

    fig = Figure(size = (1400, 1000))

    dd_year = Menu(fig[1, 1], options = collect(years))
    dd_transport = Menu(fig[1, 2], options = zip(transport_labels, transport_options) |> collect,
                        default = "Local Pricing")

    # Price axis
    ax_price = Axis(fig[2, 1:4], title = "Prices", ylabel = "€/MWh", xlabel = "Hour of Year")

    # Dispatch axes per zone
    ax_dispatch = [Axis(fig[3+i-1, 1:4], title = "Dispatch $(Zs[i])", ylabel = "Generation [MWh]",
                        xlabel = "Hour of Year") for i in 1:4]

    # Capacity bar charts
    ax_cap = [Axis(fig[7, i], title = "$(Zs[i]) Capacities", ylabel = "MW") for i in 1:4]

    sel_year = Observable(2020)
    sel_lp = Observable(true)

    on(dd_year.selection) do val
        sel_year[] = val
    end

    on(dd_transport.selection) do val
        sel_lp[] = val[2]
    end

    function update_plot(year, lp)
        r = results[year, lp]
        prices = r["prices"]
        gen = r["generation"]
        inj = r["injection"]
        ext = r["extraction"]
        fl = r["flow"]

        # Prices
        for ax in [ax_price; ax_dispatch; ax_cap]
            empty!(ax)
        end

        for (iz, z) in enumerate(Zs)
            lines!(ax_price, collect(Ts), [prices[z, t] for t in Ts],
                   color = [:blue, :red, :green, :orange][iz], label = z)
        end
        axislegend(ax_price, position = :rt)

        # Dispatch per zone
        for (iz, z) in enumerate(Zs)
            # Positive: generation + extraction + imports
            pos_keys = String[]
            pos_vals = Vector{Float64}[]
            for g in Gs
                push!(pos_keys, "Generation - $g")
                push!(pos_vals, [gen[g, z, t] for t in Ts])
            end
            for s in Ss
                push!(pos_keys, "Storage Extraction - $s")
                push!(pos_vals, [ext[s, z, t] * (s == "Battery" ? 0.95 : 0.60) for t in Ts])
            end
            push!(pos_keys, "Imports")
            push!(pos_vals, [sum(fl[z2, z, t] for z2 in Zs if z2 != z) for t in Ts])

            # Stack positive
            pos_matrix = hcat(pos_vals...)
            colors_pos = [parse(Makie.Colors.Colorant, cmap[k]) for k in pos_keys]

            cumsum_pos = cumsum(pos_matrix, dims = 2)
            xs = collect(Ts)
            for j in size(cumsum_pos, 2):-1:1
                upper = cumsum_pos[:, j]
                lower = j > 1 ? cumsum_pos[:, j-1] : zeros(length(Ts))
                band!(ax_dispatch[iz], xs, lower, upper, color = colors_pos[j])
            end

            # Negative: injection + exports (shown below zero)
            neg_data = zeros(length(Ts))
            for s in Ss
                vals = [inj[s, z, t] / (s == "Battery" ? 0.95 : 0.60) for t in Ts]
                band!(ax_dispatch[iz], xs, neg_data .- vals, neg_data,
                      color = parse(Makie.Colors.Colorant, cmap["Storage Injection - $s"]))
                neg_data .-= vals
            end
            exports = [sum(fl[z, z2, t] for z2 in Zs if z2 != z) for t in Ts]
            band!(ax_dispatch[iz], xs, neg_data .- exports, neg_data,
                  color = parse(Makie.Colors.Colorant, cmap["Exports"]))
        end

        # Capacity bar charts
        for (iz, z) in enumerate(Zs)
            gen_cap = r["generation_capacity"]
            stor_cap = r["storage_capacity"]
            inj_cap = r["injection_capacity"]
            ext_cap = r["extraction_capacity"]

            labels_cap = vcat(["Gen $g" for g in Gs],
                             ["Stor $s" for s in Ss],
                             ["Inj $s" for s in Ss],
                             ["Ext $s" for s in Ss])
            values_cap = vcat([gen_cap[g, z] for g in Gs],
                             [stor_cap[s, z] for s in Ss],
                             [inj_cap[s, z] for s in Ss],
                             [ext_cap[s, z] for s in Ss])
            barplot!(ax_cap[iz], 1:length(labels_cap), values_cap,
                     color = 1:length(labels_cap))
            ax_cap[iz].xticks = (1:length(labels_cap), labels_cap)
            ax_cap[iz].xticklabelrotation = π/4
        end
    end

    onany(sel_year, sel_lp) do year, lp
        update_plot(year, lp)
    end

    update_plot(2020, true)
    display(fig)
end


# PT.2.5 – Welfare Optimization Model
# -----------------------------------

function get_deterministic_welfare_maximization_results(optimizer)
    @load joinpath(@__DIR__, "results", "data.jld2") data

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Fs = Zs  # Each firm = TSO zone
    Ts = 1:8760
    previous = Dict(t => t == 1 ? 8760 : t - 1 for t in Ts)

    discount_rate = 0.05
    annuity(r, n) = r / (1 - (1 + r)^(-n))

    lifetime_gen = Dict("Wind offshore" => 30, "Wind onshore" => 30,
                        "Photovoltaics" => 30, "Loss of Load" => 30)
    lifetime_storage = Dict("Battery" => 15, "Hydrogen" => 30)

    capex_gen = Dict("Wind offshore" => 2.8e6, "Wind onshore" => 1.2e6,
                     "Photovoltaics" => 0.6e6, "Loss of Load" => 1.0)
    mc_gen = Dict("Wind offshore" => 1.0, "Wind onshore" => 1.0,
                  "Photovoltaics" => 1.0, "Loss of Load" => 1000.0)

    capex_storage_energy = Dict("Battery" => 3e5, "Hydrogen" => 3e3)
    capex_injection = Dict("Battery" => 1.0, "Hydrogen" => 1.4e6)
    capex_extraction = Dict("Battery" => 1.0, "Hydrogen" => 6e5)
    η_inj = Dict("Battery" => 0.95, "Hydrogen" => 0.60)
    η_ext = Dict("Battery" => 0.95, "Hydrogen" => 0.60)

    flow_cost = 1.0

    af_gen = Dict(g => annuity(discount_rate, lifetime_gen[g]) for g in Gs)
    af_storage = Dict(s => annuity(discount_rate, lifetime_storage[s]) for s in Ss)

    years = 2020:2025

    deterministic_welfare_maximization_results = JuMP.Containers.DenseAxisArray(
        Array{Any}(undef, length(years), 2), collect(years), [true, false]
    )

    for year in years
        year_data = filter(row -> row.Year == year, data)

        avail = Dict{Tuple{String,String,Int},Float64}()
        a_param = Dict{Tuple{String,Int},Float64}()
        b_param = Dict{Tuple{String,Int},Float64}()

        for row in eachrow(year_data)
            z = row.Area
            t = row.hour_of_year
            for g in ["Wind offshore", "Wind onshore", "Photovoltaics"]
                avail[(g, z, t)] = row["$(g) availability"]
            end
            avail[("Loss of Load", z, t)] = 1.0
            a_param[(z, t)] = row.a
            b_param[(z, t)] = row.b
        end

        for local_pricing in [true, false]
            model = Model(optimizer)
            set_silent(model)

            # Variables with firm dimension
            @variable(model, generation_capacity[f in Fs, g in Gs, z in Zs] >= 0)
            @variable(model, storage_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, injection_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, extraction_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, generation[f in Fs, g in Gs, z in Zs, t in Ts] >= 0)
            @variable(model, injection[f in Fs, s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, extraction[f in Fs, s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, storage_level[f in Fs, s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, flow[f in Fs, z1 in Zs, z2 in Zs, t in Ts] >= 0)
            @variable(model, Q[f in Fs, z in Zs, t in Ts] >= 0)

            # Firms can only operate in their own zone
            for f in Fs, g in Gs, z in Zs
                if f != z
                    fix(generation_capacity[f, g, z], 0.0; force = true)
                    for t in Ts
                        fix(generation[f, g, z, t], 0.0; force = true)
                    end
                end
            end
            for f in Fs, s in Ss, z in Zs
                if f != z
                    for t in Ts
                        fix(injection[f, s, z, t], 0.0; force = true)
                        fix(extraction[f, s, z, t], 0.0; force = true)
                        fix(storage_level[f, s, z, t], 0.0; force = true)
                    end
                end
            end
            for f in Fs, z1 in Zs, z2 in Zs
                if f != z1
                    for t in Ts
                        fix(flow[f, z1, z2, t], 0.0; force = true)
                    end
                end
            end

            # Objective: maximize welfare
            # Welfare = consumer surplus integral - costs
            # ∫₀^Q p(q)dq = a*Q + 0.5*b*Q²  (note b < 0, so this is concave)
            # Total Q in zone z at time t = sum_f Q[f,z,t]
            # We need: Σ_{z,t} [a[z,t]*Q_total + 0.5*b[z,t]*Q_total²] - costs
            # Q_total = Σ_f Q[f,z,t]
            # a*Q_total + 0.5*b*Q_total² = a*Σ_f Q + 0.5*b*(Σ_f Q)²
            #   = Σ_f a*Q[f] + 0.5*b*Σ_f Q[f]² + 0.5*b*Σ_{f≠f'} Q[f]*Q[f']
            #   = Σ_f [a*Q[f] + 0.5*b*Q[f]²] + b*Σ_{f<f'} Q[f]*Q[f']

            @objective(model, Max,
                sum(a_param[(z, t)] * Q[f, z, t] + 0.5 * b_param[(z, t)] * Q[f, z, t]^2
                    for f in Fs, z in Zs, t in Ts) +
                sum(b_param[(z, t)] * Q[f1, z, t] * Q[f2, z, t]
                    for f1 in Fs, f2 in Fs, z in Zs, t in Ts if f1 < f2) -
                sum(af_gen[g] * capex_gen[g] * generation_capacity[f, g, z]
                    for f in Fs, g in Gs, z in Zs) -
                sum(af_storage[s] * capex_storage_energy[s] * storage_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(af_storage[s] * capex_injection[s] * injection_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(af_storage[s] * capex_extraction[s] * extraction_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(mc_gen[g] * generation[f, g, z, t]
                    for f in Fs, g in Gs, z in Zs, t in Ts) -
                sum(flow_cost * flow[f, z1, z2, t]
                    for f in Fs, z1 in Zs, z2 in Zs, t in Ts if z1 != z2)
            )

            # Constraints
            # Generation availability
            @constraint(model, gen_avail[f in Fs, g in Gs, z in Zs, t in Ts],
                generation[f, g, z, t] <= avail[(g, z, t)] * generation_capacity[f, g, z])

            # Injection power limit: sum over firms
            @constraint(model, inj_limit[s in Ss, z in Zs, t in Ts],
                sum(injection[f, s, z, t] for f in Fs) <= injection_capacity[s, z])

            # Extraction power limit
            @constraint(model, ext_limit[s in Ss, z in Zs, t in Ts],
                sum(extraction[f, s, z, t] for f in Fs) <= extraction_capacity[s, z])

            # Storage energy capacity limit
            @constraint(model, stor_limit[s in Ss, z in Zs, t in Ts],
                sum(storage_level[f, s, z, t] for f in Fs) <= storage_capacity[s, z])

            # Storage dynamics per firm
            @constraint(model, stor_dyn[f in Fs, s in Ss, z in Zs, t in Ts],
                storage_level[f, s, z, t] == storage_level[f, s, z, previous[t]] +
                    injection[f, s, z, t] - extraction[f, s, z, t])

            # Market clearing
            @constraint(model, market_clearing[z in Zs, t in Ts],
                sum(generation[f, g, z, t] for f in Fs, g in Gs) +
                sum(extraction[f, s, z, t] * η_ext[s] for f in Fs, s in Ss) +
                sum(flow[f, z2, z, t] for f in Fs, z2 in Zs if z2 != z)
                >=
                sum(Q[f, z, t] for f in Fs) +
                sum(injection[f, s, z, t] / η_inj[s] for f in Fs, s in Ss) +
                sum(flow[f, z, z2, t] for f in Fs, z2 in Zs if z2 != z)
            )

            # Flow constraints
            if local_pricing
                @constraint(model, no_flow[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] == 0)
            else
                @constraint(model, flow_cap[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] <= 100_000)
            end

            # No self-flow
            @constraint(model, no_self_flow[f in Fs, z in Zs, t in Ts],
                flow[f, z, z, t] == 0)

            optimize!(model)
            @assert JuMP.termination_status(model) == MOI.OPTIMAL

            # Compute prices from inverse demand function
            prices_arr = JuMP.Containers.DenseAxisArray(
                [a_param[(z, t)] + b_param[(z, t)] * sum(value(Q[f, z, t]) for f in Fs)
                 for z in Zs, t in Ts],
                Zs, collect(Ts)
            )

            # Demand = sum of Q over firms
            demand_arr = JuMP.Containers.DenseAxisArray(
                [sum(value(Q[f, z, t]) for f in Fs) for z in Zs, t in Ts],
                Zs, collect(Ts)
            )

            deterministic_welfare_maximization_results[year, local_pricing] = Dict(
                "extraction"          => value.(extraction),
                "demand"              => demand_arr,
                "storage_level"       => value.(storage_level),
                "injection_capacity"  => value.(injection_capacity),
                "prices"              => prices_arr,
                "injection"           => value.(injection),
                "generation"          => value.(generation),
                "storage_capacity"    => value.(storage_capacity),
                "generation_capacity" => value.(generation_capacity),
                "total_cost"          => objective_value(model),
                "flow"                => value.(flow),
                "extraction_capacity" => value.(extraction_capacity),
            )
        end
    end

    @save joinpath(@__DIR__, "results", "deterministic_welfare_maximization_results.jld2") deterministic_welfare_maximization_results
    return nothing
end


# PT.2.6 – Strategic Behavior
# ---------------------------

function get_deterministic_strategic_behavior_results(optimizer)
    @load joinpath(@__DIR__, "results", "data.jld2") data

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Fs = Zs
    Ts = 1:8760
    previous = Dict(t => t == 1 ? 8760 : t - 1 for t in Ts)

    discount_rate = 0.05
    annuity(r, n) = r / (1 - (1 + r)^(-n))

    lifetime_gen = Dict("Wind offshore" => 30, "Wind onshore" => 30,
                        "Photovoltaics" => 30, "Loss of Load" => 30)
    lifetime_storage = Dict("Battery" => 15, "Hydrogen" => 30)

    capex_gen = Dict("Wind offshore" => 2.8e6, "Wind onshore" => 1.2e6,
                     "Photovoltaics" => 0.6e6, "Loss of Load" => 1.0)
    mc_gen = Dict("Wind offshore" => 1.0, "Wind onshore" => 1.0,
                  "Photovoltaics" => 1.0, "Loss of Load" => 1000.0)

    capex_storage_energy = Dict("Battery" => 3e5, "Hydrogen" => 3e3)
    capex_injection = Dict("Battery" => 1.0, "Hydrogen" => 1.4e6)
    capex_extraction = Dict("Battery" => 1.0, "Hydrogen" => 6e5)
    η_inj = Dict("Battery" => 0.95, "Hydrogen" => 0.60)
    η_ext = Dict("Battery" => 0.95, "Hydrogen" => 0.60)

    flow_cost = 1.0

    af_gen = Dict(g => annuity(discount_rate, lifetime_gen[g]) for g in Gs)
    af_storage = Dict(s => annuity(discount_rate, lifetime_storage[s]) for s in Ss)

    years = 2020:2025

    deterministic_strategic_behavior_results = JuMP.Containers.DenseAxisArray(
        Array{Any}(undef, length(years), 2), collect(years), [true, false]
    )

    for year in years
        year_data = filter(row -> row.Year == year, data)

        avail = Dict{Tuple{String,String,Int},Float64}()
        a_param = Dict{Tuple{String,Int},Float64}()
        b_param = Dict{Tuple{String,Int},Float64}()

        for row in eachrow(year_data)
            z = row.Area
            t = row.hour_of_year
            for g in ["Wind offshore", "Wind onshore", "Photovoltaics"]
                avail[(g, z, t)] = row["$(g) availability"]
            end
            avail[("Loss of Load", z, t)] = 1.0
            a_param[(z, t)] = row.a
            b_param[(z, t)] = row.b
        end

        for local_pricing in [true, false]
            model = Model(optimizer)
            set_silent(model)

            @variable(model, generation_capacity[f in Fs, g in Gs, z in Zs] >= 0)
            @variable(model, storage_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, injection_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, extraction_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, generation[f in Fs, g in Gs, z in Zs, t in Ts] >= 0)
            @variable(model, injection[f in Fs, s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, extraction[f in Fs, s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, storage_level[f in Fs, s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, flow[f in Fs, z1 in Zs, z2 in Zs, t in Ts] >= 0)
            @variable(model, Q[f in Fs, z in Zs, t in Ts] >= 0)
            @variable(model, Q_total[z in Zs, t in Ts] >= 0)

            # Firms only operate in their own zone
            for f in Fs, g in Gs, z in Zs
                if f != z
                    fix(generation_capacity[f, g, z], 0.0; force = true)
                    for t in Ts
                        fix(generation[f, g, z, t], 0.0; force = true)
                    end
                end
            end
            for f in Fs, s in Ss, z in Zs
                if f != z
                    for t in Ts
                        fix(injection[f, s, z, t], 0.0; force = true)
                        fix(extraction[f, s, z, t], 0.0; force = true)
                        fix(storage_level[f, s, z, t], 0.0; force = true)
                    end
                end
            end
            # Firms can only initiate flows from their own zone (same as welfare)
            for f in Fs, z1 in Zs, z2 in Zs
                if f != z1
                    for t in Ts
                        fix(flow[f, z1, z2, t], 0.0; force = true)
                    end
                end
            end
            # In local pricing, firms can only sell in their own zone
            if local_pricing
                for f in Fs, z in Zs, t in Ts
                    if f != z
                        fix(Q[f, z, t], 0.0; force = true)
                    end
                end
            end

            # Cournot convex reformulation with aggregate coefficient:
            # For N symmetric firms: Φ = Σ [a*Q_total + (N+1)/(2N)*b*Q_total²] - costs
            # lp=true: N=1 (monopoly per zone), lp=false: N=4 (all TSOs compete)
            N_firms = local_pricing ? 1 : length(Fs)
            cournot_coeff = (N_firms + 1) / (2 * N_firms)

            @constraint(model, q_total_def[z in Zs, t in Ts],
                Q_total[z, t] == sum(Q[f, z, t] for f in Fs))

            @objective(model, Max,
                sum(a_param[(z, t)] * Q_total[z, t] + cournot_coeff * b_param[(z, t)] * Q_total[z, t]^2
                    for z in Zs, t in Ts) -
                sum(af_gen[g] * capex_gen[g] * generation_capacity[f, g, z]
                    for f in Fs, g in Gs, z in Zs) -
                sum(af_storage[s] * capex_storage_energy[s] * storage_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(af_storage[s] * capex_injection[s] * injection_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(af_storage[s] * capex_extraction[s] * extraction_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(mc_gen[g] * generation[f, g, z, t]
                    for f in Fs, g in Gs, z in Zs, t in Ts) -
                sum(flow_cost * flow[f, z1, z2, t]
                    for f in Fs, z1 in Zs, z2 in Zs, t in Ts if z1 != z2)
            )

            # Same constraints as welfare model
            @constraint(model, gen_avail[f in Fs, g in Gs, z in Zs, t in Ts],
                generation[f, g, z, t] <= avail[(g, z, t)] * generation_capacity[f, g, z])

            @constraint(model, inj_limit[s in Ss, z in Zs, t in Ts],
                sum(injection[f, s, z, t] for f in Fs) <= injection_capacity[s, z])

            @constraint(model, ext_limit[s in Ss, z in Zs, t in Ts],
                sum(extraction[f, s, z, t] for f in Fs) <= extraction_capacity[s, z])

            @constraint(model, stor_limit[s in Ss, z in Zs, t in Ts],
                sum(storage_level[f, s, z, t] for f in Fs) <= storage_capacity[s, z])

            @constraint(model, stor_dyn[f in Fs, s in Ss, z in Zs, t in Ts],
                storage_level[f, s, z, t] == storage_level[f, s, z, previous[t]] +
                    injection[f, s, z, t] - extraction[f, s, z, t])

            # Aggregate zonal market clearing
            @constraint(model, market_clearing[z in Zs, t in Ts],
                sum(generation[f, g, z, t] for f in Fs, g in Gs) +
                sum(extraction[f, s, z, t] * η_ext[s] for f in Fs, s in Ss) +
                sum(flow[f, z2, z, t] for f in Fs, z2 in Zs if z2 != z)
                >=
                sum(Q[f, z, t] for f in Fs) +
                sum(injection[f, s, z, t] / η_inj[s] for f in Fs, s in Ss) +
                sum(flow[f, z, z2, t] for f in Fs, z2 in Zs if z2 != z)
            )

            # Per-firm total balance: prevent phantom sales
            @constraint(model, firm_total_balance[f in Fs, t in Ts],
                sum(Q[f, z, t] for z in Zs) ==
                sum(generation[f, g, z, t] for g in Gs, z in Zs) +
                sum(extraction[f, s, z, t] * η_ext[s] for s in Ss, z in Zs) -
                sum(injection[f, s, z, t] / η_inj[s] for s in Ss, z in Zs)
            )

            # Flow constraints (same as welfare)
            if local_pricing
                @constraint(model, no_flow[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] == 0)
            else
                @constraint(model, flow_cap[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] <= 100_000)
            end

            @constraint(model, no_self_flow[f in Fs, z in Zs, t in Ts],
                flow[f, z, z, t] == 0)

            optimize!(model)
            status = JuMP.termination_status(model)
            @assert status in (MOI.OPTIMAL, MOI.LOCALLY_SOLVED) "Strategic y=$year lp=$local_pricing: status=$status"

            prices_arr = JuMP.Containers.DenseAxisArray(
                [a_param[(z, t)] + b_param[(z, t)] * sum(value(Q[f, z, t]) for f in Fs)
                 for z in Zs, t in Ts],
                Zs, collect(Ts)
            )

            demand_arr = JuMP.Containers.DenseAxisArray(
                [sum(value(Q[f, z, t]) for f in Fs) for z in Zs, t in Ts],
                Zs, collect(Ts)
            )

            deterministic_strategic_behavior_results[year, local_pricing] = Dict(
                "extraction"          => value.(extraction),
                "demand"              => demand_arr,
                "storage_level"       => value.(storage_level),
                "injection_capacity"  => value.(injection_capacity),
                "prices"              => prices_arr,
                "injection"           => value.(injection),
                "generation"          => value.(generation),
                "storage_capacity"    => value.(storage_capacity),
                "generation_capacity" => value.(generation_capacity),
                "total_cost"          => objective_value(model),
                "flow"                => value.(flow),
                "extraction_capacity" => value.(extraction_capacity),
            )
        end
    end

    @save joinpath(@__DIR__, "results", "deterministic_strategic_behavior_results.jld2") deterministic_strategic_behavior_results
    return nothing
end


# PT.2.7 – Visualization of Elastic Models
# ----------------------------------------

function create_deterministic_elastic_results_visualization()
    welfare_path = joinpath(@__DIR__, "results", "deterministic_welfare_maximization_results.jld2")
    welfare_test = joinpath(@__DIR__, "test_data", "deterministic_welfare_maximization_results.jld2")
    welfare_results = load(isfile(welfare_path) ? welfare_path : welfare_test,
                          "deterministic_welfare_maximization_results")

    strategic_path = joinpath(@__DIR__, "results", "deterministic_strategic_behavior_results.jld2")
    strategic_test = joinpath(@__DIR__, "test_data", "deterministic_strategic_behavior_results.jld2")
    strategic_results = load(isfile(strategic_path) ? strategic_path : strategic_test,
                            "deterministic_strategic_behavior_results")

    cmap = Dict(
        "Storage Injection - Battery"    => "#ae393f",
        "Storage Injection - Hydrogen"   => "#0d47a1",
        "Storage Extraction - Battery"   => "#ae393f",
        "Storage Extraction - Hydrogen"  => "#0d47a1",
        "Imports"                        => "#4d3e35",
        "Exports"                        => "#754937",
        "Generation - Loss of Load"      => "#e54213",
        "Generation - Wind offshore"     => "#215968",
        "Generation - Wind onshore"      => "#518696",
        "Generation - Photovoltaics"     => "#ffeb3b",
    )

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Fs = Zs
    Ts = 1:8760
    years = 2020:2025
    transport_labels = ["Local Pricing", "Perfect Competition"]
    behavior_labels = ["Perfect Competition", "Strategic"]

    all_results = Dict("Perfect Competition" => welfare_results,
                       "Strategic" => strategic_results)

    fig = Figure(size = (1400, 1000))

    dd_behavior = Menu(fig[1, 1], options = behavior_labels)
    dd_transport = Menu(fig[1, 2], options = zip(transport_labels, [true, false]) |> collect,
                        default = "Local Pricing")
    dd_year = Menu(fig[1, 3], options = collect(years))

    ax_price = Axis(fig[2, 1:4], title = "Prices", ylabel = "€/MWh", xlabel = "Hour of Year")
    ax_dispatch = [Axis(fig[3+i-1, 1:4], title = "Dispatch $(Zs[i])", ylabel = "Generation [MWh]",
                        xlabel = "Hour of Year") for i in 1:4]
    ax_cap = [Axis(fig[7, i], title = "$(Zs[i]) Capacities", ylabel = "MW") for i in 1:4]

    sel_behavior = Observable("Perfect Competition")
    sel_lp = Observable(true)
    sel_year = Observable(2020)

    on(dd_behavior.selection) do val
        sel_behavior[] = val
    end
    on(dd_transport.selection) do val
        sel_lp[] = val[2]
    end
    on(dd_year.selection) do val
        sel_year[] = val
    end

    function update_plot(behavior, lp, year)
        r = all_results[behavior][year, lp]
        prices = r["prices"]
        gen_4d = r["generation"]   # [f, g, z, t]
        inj_4d = r["injection"]    # [f, s, z, t]
        ext_4d = r["extraction"]   # [f, s, z, t]
        fl_4d = r["flow"]          # [f, z1, z2, t]

        for ax in [ax_price; ax_dispatch; ax_cap]
            empty!(ax)
        end

        # Prices
        for (iz, z) in enumerate(Zs)
            lines!(ax_price, collect(Ts), [prices[z, t] for t in Ts],
                   color = [:blue, :red, :green, :orange][iz], label = z)
        end
        axislegend(ax_price, position = :rt)

        # Dispatch per zone - aggregate over firms
        for (iz, z) in enumerate(Zs)
            xs = collect(Ts)

            pos_keys = String[]
            pos_vals = Vector{Float64}[]
            for g in Gs
                push!(pos_keys, "Generation - $g")
                push!(pos_vals, [sum(gen_4d[f, g, z, t] for f in Fs) for t in Ts])
            end
            for s in Ss
                η = s == "Battery" ? 0.95 : 0.60
                push!(pos_keys, "Storage Extraction - $s")
                push!(pos_vals, [sum(ext_4d[f, s, z, t] for f in Fs) * η for t in Ts])
            end
            push!(pos_keys, "Imports")
            push!(pos_vals, [sum(fl_4d[f, z2, z, t] for f in Fs, z2 in Zs if z2 != z) for t in Ts])

            pos_matrix = hcat(pos_vals...)
            colors_pos = [parse(Makie.Colors.Colorant, cmap[k]) for k in pos_keys]
            cumsum_pos = cumsum(pos_matrix, dims = 2)

            for j in size(cumsum_pos, 2):-1:1
                upper = cumsum_pos[:, j]
                lower = j > 1 ? cumsum_pos[:, j-1] : zeros(length(Ts))
                band!(ax_dispatch[iz], xs, lower, upper, color = colors_pos[j])
            end

            neg_data = zeros(length(Ts))
            for s in Ss
                η = s == "Battery" ? 0.95 : 0.60
                vals = [sum(inj_4d[f, s, z, t] for f in Fs) / η for t in Ts]
                band!(ax_dispatch[iz], xs, neg_data .- vals, neg_data,
                      color = parse(Makie.Colors.Colorant, cmap["Storage Injection - $s"]))
                neg_data .-= vals
            end
            exports = [sum(fl_4d[f, z, z2, t] for f in Fs, z2 in Zs if z2 != z) for t in Ts]
            band!(ax_dispatch[iz], xs, neg_data .- exports, neg_data,
                  color = parse(Makie.Colors.Colorant, cmap["Exports"]))
        end

        # Capacity bars
        for (iz, z) in enumerate(Zs)
            gen_cap = r["generation_capacity"]  # [f, g, z]
            stor_cap = r["storage_capacity"]
            inj_cap = r["injection_capacity"]
            ext_cap = r["extraction_capacity"]

            labels_cap = vcat(["Gen $g" for g in Gs],
                             ["Stor $s" for s in Ss],
                             ["Inj $s" for s in Ss],
                             ["Ext $s" for s in Ss])
            values_cap = vcat([sum(gen_cap[f, g, z] for f in Fs) for g in Gs],
                             [stor_cap[s, z] for s in Ss],
                             [inj_cap[s, z] for s in Ss],
                             [ext_cap[s, z] for s in Ss])
            barplot!(ax_cap[iz], 1:length(labels_cap), values_cap,
                     color = 1:length(labels_cap))
            ax_cap[iz].xticks = (1:length(labels_cap), labels_cap)
            ax_cap[iz].xticklabelrotation = π/4
        end
    end

    onany(sel_behavior, sel_lp, sel_year) do behavior, lp, year
        update_plot(behavior, lp, year)
    end

    update_plot("Perfect Competition", true, 2020)
    display(fig)
end


# PT.2.8 – Stochastic Dual Dynamic Programming
# --------------------------------------------

function get_stochastic_cost_minimization_results(
    optimizer,
    local_pricing::Bool,
    iteration_limit::Int = 1,
    lower_bound::Float64 = -Inf,
)
    @load joinpath(@__DIR__, "results", "data.jld2") data

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Ts = 1:8760
    previous = Dict(t => t == 1 ? 8760 : t - 1 for t in Ts)

    discount_rate = 0.05
    annuity(r, n) = r / (1 - (1 + r)^(-n))

    lifetime_gen = Dict("Wind offshore" => 30, "Wind onshore" => 30,
                        "Photovoltaics" => 30, "Loss of Load" => 30)
    lifetime_storage = Dict("Battery" => 15, "Hydrogen" => 30)

    capex_gen = Dict("Wind offshore" => 2.8e6, "Wind onshore" => 1.2e6,
                     "Photovoltaics" => 0.6e6, "Loss of Load" => 1.0)
    mc_gen = Dict("Wind offshore" => 1.0, "Wind onshore" => 1.0,
                  "Photovoltaics" => 1.0, "Loss of Load" => 1000.0)

    capex_storage_energy = Dict("Battery" => 3e5, "Hydrogen" => 3e3)
    capex_injection = Dict("Battery" => 1.0, "Hydrogen" => 1.4e6)
    capex_extraction = Dict("Battery" => 1.0, "Hydrogen" => 6e5)
    η_inj = Dict("Battery" => 0.95, "Hydrogen" => 0.60)
    η_ext = Dict("Battery" => 0.95, "Hydrogen" => 0.60)

    flow_cost = 0.0

    af_gen = Dict(g => annuity(discount_rate, lifetime_gen[g]) for g in Gs)
    af_storage = Dict(s => annuity(discount_rate, lifetime_storage[s]) for s in Ss)

    years = 2020:2025

    # Pre-compute availability and demand for each year
    avail_by_year = Dict{Int,Dict{Tuple{String,String,Int},Float64}}()
    demand_by_year = Dict{Int,Dict{Tuple{String,Int},Float64}}()

    for year in years
        year_data = filter(row -> row.Year == year, data)
        avail = Dict{Tuple{String,String,Int},Float64}()
        dem = Dict{Tuple{String,Int},Float64}()
        for row in eachrow(year_data)
            z = row.Area
            t = row.hour_of_year
            for g in ["Wind offshore", "Wind onshore", "Photovoltaics"]
                avail[(g, z, t)] = row["$(g) availability"]
            end
            avail[("Loss of Load", z, t)] = 1.0
            dem[(z, t)] = row["grid load [MWh]"]
        end
        avail_by_year[year] = avail
        demand_by_year[year] = dem
    end

    model = SDDP.LinearPolicyGraph(
        stages = 2,
        lower_bound = lower_bound,
        optimizer = optimizer,
    ) do sp, stage
        set_silent(sp)

        # State variables for capacities
        @variable(sp, generation_capacity[g in Gs, z in Zs] >= 0,
                  SDDP.State, initial_value = 0.0)
        @variable(sp, storage_capacity[s in Ss, z in Zs] >= 0,
                  SDDP.State, initial_value = 0.0)
        @variable(sp, injection_capacity[s in Ss, z in Zs] >= 0,
                  SDDP.State, initial_value = 0.0)
        @variable(sp, extraction_capacity[s in Ss, z in Zs] >= 0,
                  SDDP.State, initial_value = 0.0)

        if stage == 1
            # First stage: only investment decisions
            # The .out variables represent decisions; .in is initial (0)
            @stageobjective(sp,
                sum(af_gen[g] * capex_gen[g] * generation_capacity[g, z].out
                    for g in Gs, z in Zs) +
                sum(af_storage[s] * capex_storage_energy[s] * storage_capacity[s, z].out
                    for s in Ss, z in Zs) +
                sum(af_storage[s] * capex_injection[s] * injection_capacity[s, z].out
                    for s in Ss, z in Zs) +
                sum(af_storage[s] * capex_extraction[s] * extraction_capacity[s, z].out
                    for s in Ss, z in Zs)
            )
            # Pass through: out = out (capacity decision)
            # No additional constraints needed beyond state transition
        else
            # Second stage: operational decisions
            @variable(sp, generation[g in Gs, z in Zs, t in Ts] >= 0)
            @variable(sp, injection[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(sp, extraction[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(sp, storage_level[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(sp, flow[z1 in Zs, z2 in Zs, t in Ts] >= 0)
            @variable(sp, load_shedding[z in Zs, t in Ts] >= 0)  # slack for feasibility

            # Capacities are fixed from stage 1 (state variables)
            # .in = capacity from stage 1, .out = same (no further investment)
            @constraint(sp, cap_fix_gen[g in Gs, z in Zs],
                generation_capacity[g, z].out == generation_capacity[g, z].in)
            @constraint(sp, cap_fix_stor[s in Ss, z in Zs],
                storage_capacity[s, z].out == storage_capacity[s, z].in)
            @constraint(sp, cap_fix_inj[s in Ss, z in Zs],
                injection_capacity[s, z].out == injection_capacity[s, z].in)
            @constraint(sp, cap_fix_ext[s in Ss, z in Zs],
                extraction_capacity[s, z].out == extraction_capacity[s, z].in)

            # Placeholder constraints with RHS to be set by parameterize
            @constraint(sp, gen_avail[g in Gs, z in Zs, t in Ts],
                generation[g, z, t] <= 0)
            @constraint(sp, market_clearing[z in Zs, t in Ts],
                sum(generation[g, z, t] for g in Gs) +
                sum(extraction[s, z, t] * η_ext[s] for s in Ss) +
                sum(flow[z2, z, t] for z2 in Zs if z2 != z) +
                load_shedding[z, t]
                >=
                0 +
                sum(injection[s, z, t] / η_inj[s] for s in Ss) +
                sum(flow[z, z2, t] for z2 in Zs if z2 != z)
            )

            @constraint(sp, inj_limit[s in Ss, z in Zs, t in Ts],
                injection[s, z, t] <= injection_capacity[s, z].in)
            @constraint(sp, ext_limit[s in Ss, z in Zs, t in Ts],
                extraction[s, z, t] <= extraction_capacity[s, z].in)
            @constraint(sp, stor_limit[s in Ss, z in Zs, t in Ts],
                storage_level[s, z, t] <= storage_capacity[s, z].in)
            @constraint(sp, stor_dyn[s in Ss, z in Zs, t in Ts],
                storage_level[s, z, t] == storage_level[s, z, previous[t]] +
                    injection[s, z, t] - extraction[s, z, t])

            if local_pricing
                @constraint(sp, no_flow[z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[z1, z2, t] == 0)
            else
                @constraint(sp, flow_cap[z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[z1, z2, t] <= 100_000)
            end
            @constraint(sp, no_self_flow[z in Zs, t in Ts],
                flow[z, z, t] == 0)

            SDDP.parameterize(sp, collect(years)) do ω
                avail = avail_by_year[ω]
                dem = demand_by_year[ω]

                for g in Gs, z in Zs, t in Ts
                    JuMP.set_normalized_coefficient(
                        gen_avail[g, z, t],
                        generation_capacity[g, z].in,
                        -avail[(g, z, t)]
                    )
                end
                for z in Zs, t in Ts
                    JuMP.set_normalized_rhs(market_clearing[z, t], dem[(z, t)])
                end
            end

            @stageobjective(sp,
                sum(mc_gen[g] * generation[g, z, t]
                    for g in Gs, z in Zs, t in Ts) +
                sum(flow_cost * flow[z1, z2, t]
                    for z1 in Zs, z2 in Zs, t in Ts if z1 != z2) +
                sum(1e6 * load_shedding[z, t] for z in Zs, t in Ts)
            )
        end
    end

    SDDP.train(model; iteration_limit = iteration_limit)

    # Simulate: one simulation per weather year, deterministically
    # Use Historical sampling to force each scenario
    stochastic_cost_minimization_results = []

    for year in years
        sims = SDDP.simulate(
            model, 1,
            [:generation, :injection, :extraction, :storage_level, :flow,
             :generation_capacity, :storage_capacity, :injection_capacity, :extraction_capacity],
            sampling_scheme = SDDP.Historical(
                [(1, nothing), (2, year)],
            ),
            skip_undefined_variables = true,
        )

        sim = sims[1]
        stage2 = sim[2]

        # Build demand and prices arrays
        dem = demand_by_year[year]
        demand_arr = JuMP.Containers.DenseAxisArray(
            [dem[(z, t)] for z in Zs, t in Ts],
            Zs, collect(Ts)
        )

        prices_arr = JuMP.Containers.DenseAxisArray(
            zeros(length(Zs), length(Ts)),
            Zs, collect(Ts)
        )

        stage2[:demand] = demand_arr
        stage2[:prices] = prices_arr

        push!(stochastic_cost_minimization_results, (2, stage2))
    end

    @save joinpath(@__DIR__, "results", "stochastic_cost_minimization_results.jld2") stochastic_cost_minimization_results
    return nothing
end


# PT.2.8 – Stochastic Visualization
# ---------------------------------

function create_stochastic_cost_minimization_results_visualization()
    results_path = joinpath(@__DIR__, "results", "stochastic_cost_minimization_results.jld2")
    test_path = joinpath(@__DIR__, "test_data", "stochastic_cost_minimization_results.jld2")
    path = isfile(results_path) ? results_path : test_path
    stochastic_results = load(path, "stochastic_cost_minimization_results")

    cmap = Dict(
        "Storage Injection - Battery"    => "#ae393f",
        "Storage Injection - Hydrogen"   => "#0d47a1",
        "Storage Extraction - Battery"   => "#ae393f",
        "Storage Extraction - Hydrogen"  => "#0d47a1",
        "Imports"                        => "#4d3e35",
        "Exports"                        => "#754937",
        "Generation - Loss of Load"      => "#e54213",
        "Generation - Wind offshore"     => "#215968",
        "Generation - Wind onshore"      => "#518696",
        "Generation - Photovoltaics"     => "#ffeb3b",
    )

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Ts = 1:8760

    # Map noise_term (year) to result index
    year_to_idx = Dict{Int,Int}()
    for (idx, item) in enumerate(stochastic_results)
        year_to_idx[item[2][:noise_term]] = idx
    end
    available_years = sort(collect(keys(year_to_idx)))

    fig = Figure(size = (1400, 1000))
    dd_year = Menu(fig[1, 1], options = available_years)

    ax_price = Axis(fig[2, 1:4], title = "Prices", ylabel = "€/MWh", xlabel = "Hour of Year")
    ax_dispatch = [Axis(fig[3+i-1, 1:4], title = "Dispatch $(Zs[i])", ylabel = "Generation [MWh]",
                        xlabel = "Hour of Year") for i in 1:4]
    ax_cap = [Axis(fig[7, i], title = "$(Zs[i]) Capacities", ylabel = "MW") for i in 1:4]

    function update_plot(year)
        idx = year_to_idx[year]
        r = stochastic_results[idx][2]
        prices = r[:prices]
        gen = r[:generation]
        inj = r[:injection]
        ext = r[:extraction]
        fl = r[:flow]

        for ax in [ax_price; ax_dispatch; ax_cap]
            empty!(ax)
        end

        for (iz, z) in enumerate(Zs)
            lines!(ax_price, collect(Ts), [prices[z, t] for t in Ts],
                   color = [:blue, :red, :green, :orange][iz], label = z)
        end
        axislegend(ax_price, position = :rt)

        for (iz, z) in enumerate(Zs)
            xs = collect(Ts)
            pos_keys = String[]
            pos_vals = Vector{Float64}[]
            for g in Gs
                push!(pos_keys, "Generation - $g")
                push!(pos_vals, [gen[g, z, t] for t in Ts])
            end
            for s in Ss
                η = s == "Battery" ? 0.95 : 0.60
                push!(pos_keys, "Storage Extraction - $s")
                push!(pos_vals, [ext[s, z, t] * η for t in Ts])
            end
            push!(pos_keys, "Imports")
            push!(pos_vals, [sum(fl[z2, z, t] for z2 in Zs if z2 != z) for t in Ts])

            pos_matrix = hcat(pos_vals...)
            colors_pos = [parse(Makie.Colors.Colorant, cmap[k]) for k in pos_keys]
            cumsum_pos = cumsum(pos_matrix, dims = 2)

            for j in size(cumsum_pos, 2):-1:1
                upper = cumsum_pos[:, j]
                lower = j > 1 ? cumsum_pos[:, j-1] : zeros(length(Ts))
                band!(ax_dispatch[iz], xs, lower, upper, color = colors_pos[j])
            end

            neg_data = zeros(length(Ts))
            for s in Ss
                η = s == "Battery" ? 0.95 : 0.60
                vals = [inj[s, z, t] / η for t in Ts]
                band!(ax_dispatch[iz], xs, neg_data .- vals, neg_data,
                      color = parse(Makie.Colors.Colorant, cmap["Storage Injection - $s"]))
                neg_data .-= vals
            end
            exports = [sum(fl[z, z2, t] for z2 in Zs if z2 != z) for t in Ts]
            band!(ax_dispatch[iz], xs, neg_data .- exports, neg_data,
                  color = parse(Makie.Colors.Colorant, cmap["Exports"]))
        end

        for (iz, z) in enumerate(Zs)
            gen_cap = r[:generation_capacity]
            stor_cap = r[:storage_capacity]
            inj_cap = r[:injection_capacity]
            ext_cap = r[:extraction_capacity]

            labels_cap = vcat(["Gen $g" for g in Gs],
                             ["Stor $s" for s in Ss],
                             ["Inj $s" for s in Ss],
                             ["Ext $s" for s in Ss])
            values_cap = vcat([gen_cap[g, z].out for g in Gs],
                             [stor_cap[s, z].out for s in Ss],
                             [inj_cap[s, z].out for s in Ss],
                             [ext_cap[s, z].out for s in Ss])
            barplot!(ax_cap[iz], 1:length(labels_cap), values_cap,
                     color = 1:length(labels_cap))
            ax_cap[iz].xticks = (1:length(labels_cap), labels_cap)
            ax_cap[iz].xticklabelrotation = π/4
        end
    end

    on(dd_year.selection) do year
        update_plot(year)
    end

    update_plot(available_years[1])
    display(fig)
end