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


# Some Functions
# --------------

const generation_technologies = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
const storage_technologies = ["Battery", "Hydrogen"]
const zones = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
const time_steps = 1:8760
const weather_years = 2020:2025

const previous_time_step = Dict(t => t == 1 ? 8760 : t - 1 for t in time_steps)

annuity(discount_rate, lifetime_years) = discount_rate / (1 - (1 + discount_rate)^(-lifetime_years))

function get_model_parameters()
    discount_rate = 0.05

    generation_lifetime_years = Dict(
        "Wind offshore" => 30,
        "Wind onshore" => 30,
        "Photovoltaics" => 30,
        "Loss of Load" => 30,
    )

    storage_lifetime_years = Dict(
        "Battery" => 15,
        "Hydrogen" => 30,
    )

    generation_investment_cost_per_mw = Dict(
        "Wind offshore" => 2.8e6,
        "Wind onshore" => 1.2e6,
        "Photovoltaics" => 0.6e6,
        "Loss of Load" => 1.0,
    )

    generation_marginal_cost_per_mwh = Dict(
        "Wind offshore" => 1.0,
        "Wind onshore" => 1.0,
        "Photovoltaics" => 1.0,
        "Loss of Load" => 1000.0,
    )

    storage_energy_investment_cost_per_mwh = Dict(
        "Battery" => 3e5,
        "Hydrogen" => 3e3,
    )

    storage_injection_investment_cost_per_mw = Dict(
        "Battery" => 1.0,
        "Hydrogen" => 1.4e6,
    )

    storage_extraction_investment_cost_per_mw = Dict(
        "Battery" => 1.0,
        "Hydrogen" => 6e5,
    )

    charging_efficiency = Dict(
        "Battery" => 0.95,
        "Hydrogen" => 0.60,
    )

    discharging_efficiency = Dict(
        "Battery" => 0.95,
        "Hydrogen" => 0.60,
    )

    transmission_flow_cost_per_mwh = 1.0

    generation_annuity_factor = Dict(
        technology => annuity(discount_rate, generation_lifetime_years[technology])
        for technology in generation_technologies
    )

    storage_annuity_factor = Dict(
        technology => annuity(discount_rate, storage_lifetime_years[technology])
        for technology in storage_technologies
    )

    return (
        discount_rate = discount_rate,
        generation_lifetime_years = generation_lifetime_years,
        storage_lifetime_years = storage_lifetime_years,
        generation_investment_cost_per_mw = generation_investment_cost_per_mw,
        generation_marginal_cost_per_mwh = generation_marginal_cost_per_mwh,
        storage_energy_investment_cost_per_mwh = storage_energy_investment_cost_per_mwh,
        storage_injection_investment_cost_per_mw = storage_injection_investment_cost_per_mw,
        storage_extraction_investment_cost_per_mw = storage_extraction_investment_cost_per_mw,
        charging_efficiency = charging_efficiency,
        discharging_efficiency = discharging_efficiency,
        transmission_flow_cost_per_mwh = transmission_flow_cost_per_mwh,
        generation_annuity_factor = generation_annuity_factor,
        storage_annuity_factor = storage_annuity_factor,
    )
end

function prepare_yearly_model_inputs(data)
    year_cache = Dict{Int, NamedTuple}()

    for year in weather_years
        year_data = filter(row -> row.Year == year, data)

        generation_availability = Dict{Tuple{String,String,Int},Float64}()
        demand_by_zone_and_time = Dict{Tuple{String,Int},Float64}()
        inverse_demand_intercept = Dict{Tuple{String,Int},Float64}()
        inverse_demand_slope = Dict{Tuple{String,Int},Float64}()

        for row in eachrow(year_data)
            zone = row.Area
            time = row.hour_of_year

            for technology in ["Wind offshore", "Wind onshore", "Photovoltaics"]
                generation_availability[(technology, zone, time)] =
                    row["$(technology) availability"]
            end

            generation_availability[("Loss of Load", zone, time)] = 1.0
            demand_by_zone_and_time[(zone, time)] = row["grid load [MWh]"]
            inverse_demand_intercept[(zone, time)] = row.a
            inverse_demand_slope[(zone, time)] = row.b
        end

        year_cache[year] = (
            year_data = year_data,
            generation_availability = generation_availability,
            demand_by_zone_and_time = demand_by_zone_and_time,
            inverse_demand_intercept = inverse_demand_intercept,
            inverse_demand_slope = inverse_demand_slope,
        )
    end

    return year_cache
end

# PT.2.3 – Cost Optimization Model
# --------------------------------

function get_deterministic_cost_minimization_results(optimizer)
    @load joinpath(@__DIR__, "results", "data.jld2") data

    Gs = generation_technologies
    Ss = storage_technologies
    Zs = zones
    Ts = time_steps
    previous = previous_time_step
    years = weather_years

    model_parameters = get_model_parameters()
    yearly_model_inputs = prepare_yearly_model_inputs(data)

    deterministic_cost_minimization_results = JuMP.Containers.DenseAxisArray(
        Array{Any}(undef, length(years), 2), collect(years), [true, false]
    )

    for year in years
        year_data = yearly_model_inputs[year]
        generation_availability = year_data.generation_availability
        demand_by_zone_and_time = year_data.demand_by_zone_and_time

        for local_pricing in [true, false]
            model = Model(optimizer)
            set_silent(model)

            @variable(model, generation_capacity[g in Gs, z in Zs] >= 0)
            @variable(model, storage_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, injection_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, extraction_capacity[s in Ss, z in Zs] >= 0)
            @variable(model, generation[g in Gs, z in Zs, t in Ts] >= 0)
            @variable(model, injection[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, extraction[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, storage_level[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(model, flow[z1 in Zs, z2 in Zs, t in Ts] >= 0)

            @objective(model, Min,
                sum(model_parameters.generation_annuity_factor[g] * model_parameters.generation_investment_cost_per_mw[g] * generation_capacity[g, z] for g in Gs, z in Zs) +
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_energy_investment_cost_per_mwh[s] * storage_capacity[s, z] for s in Ss, z in Zs) +
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_injection_investment_cost_per_mw[s] * injection_capacity[s, z] for s in Ss, z in Zs) +
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_extraction_investment_cost_per_mw[s] * extraction_capacity[s, z] for s in Ss, z in Zs) +
                sum(model_parameters.generation_marginal_cost_per_mwh[g] * generation[g, z, t] for g in Gs, z in Zs, t in Ts) +
                sum(model_parameters.transmission_flow_cost_per_mwh * flow[z1, z2, t] for z1 in Zs, z2 in Zs, t in Ts if z1 != z2)
            )

            @constraint(model, generation_availability_constraint[g in Gs, z in Zs, t in Ts],
                generation[g, z, t] <= generation_availability[(g, z, t)] * generation_capacity[g, z])

            @constraint(model, injection_capacity_constraint[s in Ss, z in Zs, t in Ts],
                injection[s, z, t] <= injection_capacity[s, z])

            @constraint(model, extraction_capacity_constraint[s in Ss, z in Zs, t in Ts],
                extraction[s, z, t] <= extraction_capacity[s, z])

            @constraint(model, storage_capacity_constraint[s in Ss, z in Zs, t in Ts],
                storage_level[s, z, t] <= storage_capacity[s, z])

            @constraint(model, storage_balance_constraint[s in Ss, z in Zs, t in Ts],
                storage_level[s, z, t] == storage_level[s, z, previous[t]] + injection[s, z, t] - extraction[s, z, t])

            @constraint(model, market_clearing[z in Zs, t in Ts],
                sum(generation[g, z, t] for g in Gs) +
                sum(extraction[s, z, t] * model_parameters.discharging_efficiency[s] for s in Ss) +
                sum(flow[z2, z, t] for z2 in Zs if z2 != z)
                >=
                demand_by_zone_and_time[(z, t)] +
                sum(injection[s, z, t] / model_parameters.charging_efficiency[s] for s in Ss) +
                sum(flow[z, z2, t] for z2 in Zs if z2 != z)
            )

            if local_pricing
                @constraint(model, no_flow[z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[z1, z2, t] == 0)
            else
                @constraint(model, flow_capacity_constraint[z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[z1, z2, t] <= 100_000)
            end

            @constraint(model, no_self_flow[z in Zs, t in Ts],
                flow[z, z, t] == 0)

            optimize!(model)
            @assert JuMP.termination_status(model) == MOI.OPTIMAL

            prices = JuMP.Containers.DenseAxisArray(
                [dual(market_clearing[z, t]) for z in Zs, t in Ts],
                Zs, collect(Ts))

            demand_array = JuMP.Containers.DenseAxisArray(
                [demand_by_zone_and_time[(z, t)] for z in Zs, t in Ts],
                Zs, collect(Ts))

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
    results = load(results_path, "deterministic_cost_minimization_results")

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

    zone_colors = [colorant"#1565c0", colorant"#f9a825", colorant"#2e7d32", colorant"#8e24aa"]

    Gs = ["Wind offshore", "Wind onshore", "Photovoltaics", "Loss of Load"]
    Ss = ["Battery", "Hydrogen"]
    Zs = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
    Ts = 1:8760
    years = 2020:2025
    transport_options = [true, false]
    transport_labels = ["Local Pricing", "Perfect Competition"]

    fig = Figure(size = (1600, 1200))

    dd_transport = Menu(fig[1, 1], options = zip(transport_labels, transport_options) |> collect,
                        default = "Local Pricing")
    dd_year = Menu(fig[1, 2], options = collect(years))

    # Price axis
    ax_price = Axis(fig[2, 1:4], title = "Prices", ylabel = "Price [€/MWh]", xlabel = "Hour of Year")

    # Dispatch axes per zone
    ax_dispatch = [Axis(fig[2+i, 1:4], title = "Dispatch $(Zs[i])", ylabel = "Generation [MW]",
                        xlabel = "Hour of Year") for i in 1:4]

    # Shared dispatch legend on the right
    legend_order = ["Exports", "Storage Injection - Battery", "Storage Injection - Hydrogen",
                    "Imports", "Storage Extraction - Battery", "Storage Extraction - Hydrogen",
                    "Generation - Wind offshore", "Generation - Wind onshore",
                    "Generation - Photovoltaics", "Generation - Loss of Load"]
    legend_elements = [PolyElement(color = parse(Makie.Colors.Colorant, cmap[k])) for k in legend_order]
    Legend(fig[3:6, 5], legend_elements, legend_order, framevisible = true, labelsize = 10)

    ax_gen_cap = [Axis(fig[7, i], title = "$(Zs[i]) Capacities") for i in 1:4]
    ax_inj_cap = [Axis(fig[8, i]) for i in 1:4]
    ax_ext_cap = [Axis(fig[9, i]) for i in 1:4]
    ax_str_cap = [Axis(fig[10, i]) for i in 1:4]
    ax_gen_cap[1].ylabel = "Gen [MW]"
    ax_inj_cap[1].ylabel = "Inj [MW]"
    ax_ext_cap[1].ylabel = "Ext [MW]"
    ax_str_cap[1].ylabel = "Str [MWh]"
    for r in 7:10; rowsize!(fig.layout, r, Fixed(100)); end

    all_cap_axes = vcat(ax_gen_cap, ax_inj_cap, ax_ext_cap, ax_str_cap)

    zone_legend_elements = [LineElement(color = zone_colors[iz]) for iz in 1:4]
    Legend(fig[2, 5], zone_legend_elements, Zs, framevisible = true, labelsize = 10)

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

        for ax in [ax_price; ax_dispatch; all_cap_axes]
            empty!(ax)
        end

        # Prices
        for (iz, z) in enumerate(Zs)
            lines!(ax_price, collect(Ts), [prices[z, t] for t in Ts],
                   color = zone_colors[iz])
        end

        # Dispatch per zone
        for (iz, z) in enumerate(Zs)
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

            pos_matrix = hcat(pos_vals...)
            colors_pos = [parse(Makie.Colors.Colorant, cmap[k]) for k in pos_keys]
            cumsum_pos = cumsum(pos_matrix, dims = 2)
            xs = collect(Ts)
            for j in size(cumsum_pos, 2):-1:1
                upper = cumsum_pos[:, j]
                lower = j > 1 ? cumsum_pos[:, j-1] : zeros(length(Ts))
                band!(ax_dispatch[iz], xs, lower, upper, color = colors_pos[j])
            end

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
            hlines!(ax_dispatch[iz], [0], color = :black, linewidth = 0.5)
        end

        # Capacity bar charts by type
        gen_cap = r["generation_capacity"]
        stor_cap = r["storage_capacity"]
        inj_cap_d = r["injection_capacity"]
        ext_cap_d = r["extraction_capacity"]

        gen_cols = [parse(Makie.Colors.Colorant, cmap["Generation - $g"]) for g in Gs]
        stor_inj_cols = [parse(Makie.Colors.Colorant, cmap["Storage Injection - $s"]) for s in Ss]
        stor_ext_cols = [parse(Makie.Colors.Colorant, cmap["Storage Extraction - $s"]) for s in Ss]

        for (iz, z) in enumerate(Zs)
            barplot!(ax_gen_cap[iz], 1:4, [gen_cap[g, z] for g in Gs], color = gen_cols)
            ax_gen_cap[iz].xticks = (1:4, ["W.off", "W.on", "PV", "LoL"])
            ax_gen_cap[iz].xticklabelrotation = π/4

            barplot!(ax_inj_cap[iz], 1:2, [inj_cap_d[s, z] for s in Ss], color = stor_inj_cols)
            ax_inj_cap[iz].xticks = (1:2, ["Bat", "H₂"])

            barplot!(ax_ext_cap[iz], 1:2, [ext_cap_d[s, z] for s in Ss], color = stor_ext_cols)
            ax_ext_cap[iz].xticks = (1:2, ["Bat", "H₂"])

            barplot!(ax_str_cap[iz], 1:2, [stor_cap[s, z] for s in Ss], color = stor_ext_cols)
            ax_str_cap[iz].xticks = (1:2, ["Bat", "H₂"])
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

    Gs = generation_technologies
    Ss = storage_technologies
    Zs = zones
    Fs = zones
    Ts = time_steps
    years = weather_years
    previous = previous_time_step

    model_parameters = get_model_parameters()
    yearly_model_inputs = prepare_yearly_model_inputs(data)

    deterministic_welfare_maximization_results = JuMP.Containers.DenseAxisArray(
        Array{Any}(undef, length(years), 2), collect(years), [true, false])

    for year in years
        year_data = yearly_model_inputs[year]
        generation_availability = year_data.generation_availability
        inverse_demand_intercept = year_data.inverse_demand_intercept
        inverse_demand_slope = year_data.inverse_demand_slope

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

            @objective(model, Max,
                sum(inverse_demand_intercept[(z, t)] * Q[f, z, t] + 0.5 * inverse_demand_slope[(z, t)] * Q[f, z, t]^2
                    for f in Fs, z in Zs, t in Ts) +
                sum(inverse_demand_slope[(z, t)] * Q[f1, z, t] * Q[f2, z, t]
                    for f1 in Fs, f2 in Fs, z in Zs, t in Ts if f1 < f2) -
                sum(model_parameters.generation_annuity_factor[g] * model_parameters.generation_investment_cost_per_mw[g] * generation_capacity[f, g, z]
                    for f in Fs, g in Gs, z in Zs) -
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_energy_investment_cost_per_mwh[s] * storage_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_injection_investment_cost_per_mw[s] * injection_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_extraction_investment_cost_per_mw[s] * extraction_capacity[s, z]
                    for s in Ss, z in Zs) -
                sum(model_parameters.generation_marginal_cost_per_mwh[g] * generation[f, g, z, t]
                    for f in Fs, g in Gs, z in Zs, t in Ts) -
                sum(model_parameters.transmission_flow_cost_per_mwh *flow[f, z1, z2, t]
                    for f in Fs, z1 in Zs, z2 in Zs, t in Ts if z1 != z2)
            )

            @constraint(model, generation_availability_constraint[f in Fs, g in Gs, z in Zs, t in Ts],
                generation[f, g, z, t] <= generation_availability[(g, z, t)] * generation_capacity[f, g, z])

            @constraint(model, injection_capacity_constraint[s in Ss, z in Zs, t in Ts],
                sum(injection[f, s, z, t] for f in Fs) <= injection_capacity[s, z])

            @constraint(model, extraction_capacity_constraint[s in Ss, z in Zs, t in Ts],
                sum(extraction[f, s, z, t] for f in Fs) <= extraction_capacity[s, z])

            @constraint(model, storage_capacity_constraint[s in Ss, z in Zs, t in Ts],
                sum(storage_level[f, s, z, t] for f in Fs) <= storage_capacity[s, z])

            @constraint(model, storage_balance_constraint[f in Fs, s in Ss, z in Zs, t in Ts],
                storage_level[f, s, z, t] == storage_level[f, s, z, previous[t]] + injection[f, s, z, t] - extraction[f, s, z, t])

            @constraint(model, market_clearing[z in Zs, t in Ts],
                sum(generation[f, g, z, t] for f in Fs, g in Gs) +
                sum(extraction[f, s, z, t] * model_parameters.discharging_efficiency[s] for f in Fs, s in Ss) +
                sum(flow[f, z2, z, t] for f in Fs, z2 in Zs if z2 != z)
                >=
                sum(Q[f, z, t] for f in Fs) +
                sum(injection[f, s, z, t] / model_parameters.charging_efficiency[s] for f in Fs, s in Ss) +
                sum(flow[f, z, z2, t] for f in Fs, z2 in Zs if z2 != z)
            )

            if local_pricing
                @constraint(model, no_flow[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] == 0)
            else
                @constraint(model, flow_capacity_constraint[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] <= 100_000)
            end

            @constraint(model, no_self_flow[f in Fs, z in Zs, t in Ts],
                flow[f, z, z, t] == 0
            )

            optimize!(model)
            @assert JuMP.termination_status(model) == MOI.OPTIMAL

            prices_arr = JuMP.Containers.DenseAxisArray(
                [
                    inverse_demand_intercept[(z, t)] +
                    inverse_demand_slope[(z, t)] * sum(value(Q[f, z, t]) for f in Fs)
                    for z in Zs, t in Ts
                ],
                Zs, collect(Ts)
            )

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

    Gs = generation_technologies
    Ss = storage_technologies
    Zs = zones
    Fs = zones
    Ts = time_steps
    years = weather_years
    previous = previous_time_step

    model_parameters = get_model_parameters()
    yearly_model_inputs = prepare_yearly_model_inputs(data)

    deterministic_strategic_behavior_results = JuMP.Containers.DenseAxisArray(
        Array{Any}(undef, length(years), 2), collect(years), [true, false]
    )

    for year in years
        year_information = yearly_model_inputs[year]
        generation_availability = year_information.generation_availability
        inverse_demand_intercept = year_information.inverse_demand_intercept
        inverse_demand_slope = year_information.inverse_demand_slope

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

            for f in Fs, g in Gs, z in Zs
                if f != z
                    fix(generation_capacity[f, g, z], 0.0; force = true)
                    for t in Ts
                        fix(generation[f, g, z, t], 0.0; force = true)
                    end
                end
            end

            if local_pricing
                for f in Fs, z in Zs, t in Ts
                    if f != z
                        fix(Q[f, z, t], 0.0; force = true)
                    end
                end
            end

            @expression(model, total_quantity[z in Zs, t in Ts],
                sum(Q[f, z, t] for f in Fs)
            )

            @objective(model, Max,
                sum(inverse_demand_intercept[(z, t)] * total_quantity[z, t] + 0.5 * inverse_demand_slope[(z, t)] * total_quantity[z, t]^2 for z in Zs, t in Ts) +
                sum(0.5 * inverse_demand_slope[(z, t)] * Q[f, z, t]^2 for f in Fs, z in Zs, t in Ts)-
                sum(model_parameters.generation_annuity_factor[g] * model_parameters.generation_investment_cost_per_mw[g] * generation_capacity[f, g, z] for f in Fs, g in Gs, z in Zs)-
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_energy_investment_cost_per_mwh[s] * storage_capacity[s, z] for s in Ss, z in Zs)-
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_injection_investment_cost_per_mw[s] * injection_capacity[s, z] for s in Ss, z in Zs) -
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_extraction_investment_cost_per_mw[s] * extraction_capacity[s, z] for s in Ss, z in Zs) -
                sum(model_parameters.generation_marginal_cost_per_mwh[g] *generation[f, g, z, t] for f in Fs, g in Gs, z in Zs, t in Ts)-
                sum(model_parameters.transmission_flow_cost_per_mwh * flow[f, z1, z2, t] for f in Fs, z1 in Zs, z2 in Zs, t in Ts if z1 != z2)
            )

            @constraint(model, generation_availability_constraint[f in Fs, g in Gs, z in Zs, t in Ts],
                generation[f, g, z, t] <= generation_availability[(g, z, t)] * generation_capacity[f, g, z])

            @constraint(model, injection_capacity_constraint[s in Ss, z in Zs, t in Ts],
                sum(injection[f, s, z, t] for f in Fs) <= injection_capacity[s, z])

            @constraint(model, extraction_capacity_constraint[s in Ss, z in Zs, t in Ts],
                sum(extraction[f, s, z, t] for f in Fs) <= extraction_capacity[s, z])

            @constraint(model, storage_capacity_constraint[s in Ss, z in Zs, t in Ts],
                sum(storage_level[f, s, z, t] for f in Fs) <= storage_capacity[s, z])

            @constraint(model, storage_balance_constraint[f in Fs, s in Ss, z in Zs, t in Ts],
                storage_level[f, s, z, t] == storage_level[f, s, z, previous[t]] + injection[f, s, z, t] - extraction[f, s, z, t])

            @constraint(model, firm_balance_constraint[f in Fs, z in Zs, t in Ts],
                sum(generation[f, g, z, t] for g in Gs) +
                sum(extraction[f, s, z, t] * model_parameters.discharging_efficiency[s] for s in Ss) +
                sum(flow[f, z2, z, t] for z2 in Zs if z2 != z)
                ==
                Q[f, z, t] +
                sum(injection[f, s, z, t] / model_parameters.charging_efficiency[s] for s in Ss) +
                sum(flow[f, z, z2, t] for z2 in Zs if z2 != z)
            )

            if local_pricing
                @constraint(model, no_flow[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] == 0)
            else
                @constraint(model, flow_capacity_constraint[f in Fs, z1 in Zs, z2 in Zs, t in Ts; z1 != z2],
                    flow[f, z1, z2, t] <= 100_000)
            end

            @constraint(model, no_self_flow[f in Fs, z in Zs, t in Ts],
                flow[f, z, z, t] == 0)

            optimize!(model)
            status = JuMP.termination_status(model)
            @assert status in (MOI.OPTIMAL, MOI.LOCALLY_SOLVED) "Strategic y=$year lp=$local_pricing: status=$status"

            prices_arr = JuMP.Containers.DenseAxisArray(
                [
                    inverse_demand_intercept[(z, t)] +
                    inverse_demand_slope[(z, t)] * value(total_quantity[z, t])
                    for z in Zs, t in Ts
                ],
                Zs, collect(Ts)
            )

            demand_arr = JuMP.Containers.DenseAxisArray(
                [value(total_quantity[z, t]) for z in Zs, t in Ts],
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
    welfare_results = load(welfare_path, "deterministic_welfare_maximization_results")

    strategic_path = joinpath(@__DIR__, "results", "deterministic_strategic_behavior_results.jld2")
    strategic_results = load(strategic_path, "deterministic_strategic_behavior_results")

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

    zone_colors = [colorant"#1565c0", colorant"#f9a825", colorant"#2e7d32", colorant"#8e24aa"]

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

    fig = Figure(size = (1600, 1200))

    dd_behavior = Menu(fig[1, 1], options = behavior_labels)
    dd_transport = Menu(fig[1, 2], options = zip(transport_labels, [true, false]) |> collect,
                        default = "Local Pricing")
    dd_year = Menu(fig[1, 3], options = collect(years))

    ax_price = Axis(fig[2, 1:4], title = "Prices", ylabel = "Price [€/MWh]", xlabel = "Hour of Year")
    ax_dispatch = [Axis(fig[2+i, 1:4], title = "Dispatch $(Zs[i])", ylabel = "Generation [MW]",
                        xlabel = "Hour of Year") for i in 1:4]

    # Shared dispatch legend on the right
    legend_order = ["Exports", "Storage Injection - Battery", "Storage Injection - Hydrogen",
                    "Imports", "Storage Extraction - Battery", "Storage Extraction - Hydrogen",
                    "Generation - Wind offshore", "Generation - Wind onshore",
                    "Generation - Photovoltaics", "Generation - Loss of Load"]
    legend_elements = [PolyElement(color = parse(Makie.Colors.Colorant, cmap[k])) for k in legend_order]
    Legend(fig[3:6, 5], legend_elements, legend_order, framevisible = true, labelsize = 10)

    # Capacity bar charts: 4 rows × 4 zones
    ax_gen_cap = [Axis(fig[7, i], title = "$(Zs[i]) Capacities") for i in 1:4]
    ax_inj_cap = [Axis(fig[8, i]) for i in 1:4]
    ax_ext_cap = [Axis(fig[9, i]) for i in 1:4]
    ax_str_cap = [Axis(fig[10, i]) for i in 1:4]
    ax_gen_cap[1].ylabel = "Gen [MW]"
    ax_inj_cap[1].ylabel = "Inj [MW]"
    ax_ext_cap[1].ylabel = "Ext [MW]"
    ax_str_cap[1].ylabel = "Str [MWh]"
    for r in 7:10; rowsize!(fig.layout, r, Fixed(100)); end

    all_cap_axes = vcat(ax_gen_cap, ax_inj_cap, ax_ext_cap, ax_str_cap)

    zone_legend_elements = [LineElement(color = zone_colors[iz]) for iz in 1:4]
    Legend(fig[2, 5], zone_legend_elements, Zs, framevisible = true, labelsize = 10)

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

        for ax in [ax_price; ax_dispatch; all_cap_axes]
            empty!(ax)
        end

        # Prices
        for (iz, z) in enumerate(Zs)
            lines!(ax_price, collect(Ts), [prices[z, t] for t in Ts],
                   color = zone_colors[iz])
        end

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
            hlines!(ax_dispatch[iz], [0], color = :black, linewidth = 0.5)
        end

        # Capacity bars by type
        gen_cap = r["generation_capacity"]
        stor_cap = r["storage_capacity"]
        inj_cap_d = r["injection_capacity"]
        ext_cap_d = r["extraction_capacity"]

        gen_cols = [parse(Makie.Colors.Colorant, cmap["Generation - $g"]) for g in Gs]
        stor_inj_cols = [parse(Makie.Colors.Colorant, cmap["Storage Injection - $s"]) for s in Ss]
        stor_ext_cols = [parse(Makie.Colors.Colorant, cmap["Storage Extraction - $s"]) for s in Ss]

        for (iz, z) in enumerate(Zs)
            barplot!(ax_gen_cap[iz], 1:4, [sum(gen_cap[f, g, z] for f in Fs) for g in Gs], color = gen_cols)
            ax_gen_cap[iz].xticks = (1:4, ["W.off", "W.on", "PV", "LoL"])
            ax_gen_cap[iz].xticklabelrotation = π/4

            barplot!(ax_inj_cap[iz], 1:2, [inj_cap_d[s, z] for s in Ss], color = stor_inj_cols)
            ax_inj_cap[iz].xticks = (1:2, ["Bat", "H₂"])

            barplot!(ax_ext_cap[iz], 1:2, [ext_cap_d[s, z] for s in Ss], color = stor_ext_cols)
            ax_ext_cap[iz].xticks = (1:2, ["Bat", "H₂"])

            barplot!(ax_str_cap[iz], 1:2, [stor_cap[s, z] for s in Ss], color = stor_ext_cols)
            ax_str_cap[iz].xticks = (1:2, ["Bat", "H₂"])
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

    Gs = generation_technologies
    Ss = storage_technologies
    Zs = zones
    Ts = time_steps
    years = weather_years
    previous = previous_time_step

    model_parameters = get_model_parameters()
    yearly_model_inputs = prepare_yearly_model_inputs(data)

    generation_availability_by_year = Dict{Int, Dict{Tuple{String,String,Int},Float64}}()
    demand_by_year = Dict{Int, Dict{Tuple{String,Int},Float64}}()

    for year in years
        year_information = yearly_model_inputs[year]
        generation_availability_by_year[year] = year_information.generation_availability
        demand_by_year[year] = year_information.demand_by_zone_and_time
    end

    stochastic_model = SDDP.LinearPolicyGraph(
        stages = 2,
        lower_bound = lower_bound,
        optimizer = optimizer,
    ) do subproblem, stage
        set_silent(subproblem)

        @variable(subproblem, generation_capacity[g in Gs, z in Zs] >= 0, SDDP.State,initial_value = 0.0,)
        @variable(subproblem, storage_capacity[s in Ss, z in Zs] >= 0, SDDP.State, initial_value = 0.0,)
        @variable(subproblem, injection_capacity[s in Ss, z in Zs] >= 0, SDDP.State, initial_value = 0.0,)
        @variable(subproblem, extraction_capacity[s in Ss, z in Zs] >= 0, SDDP.State, initial_value = 0.0,)

        if stage == 1
            @stageobjective(
                subproblem,
                sum(model_parameters.generation_annuity_factor[g] * model_parameters.generation_investment_cost_per_mw[g] * generation_capacity[g, z].out for g in Gs, z in Zs) +
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_energy_investment_cost_per_mwh[s] * storage_capacity[s, z].out for s in Ss, z in Zs ) +
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_injection_investment_cost_per_mw[s] * injection_capacity[s, z].out for s in Ss, z in Zs) +
                sum(model_parameters.storage_annuity_factor[s] * model_parameters.storage_extraction_investment_cost_per_mw[s] * extraction_capacity[s, z].out for s in Ss, z in Zs)
            )

        else
            @variable(subproblem, generation[g in Gs, z in Zs, t in Ts] >= 0)
            @variable(subproblem, injection[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(subproblem, extraction[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(subproblem, storage_level[s in Ss, z in Zs, t in Ts] >= 0)
            @variable(subproblem, flow[z1 in Zs, z2 in Zs, t in Ts] >= 0)
            @variable(subproblem, load_shedding[z in Zs, t in Ts] >= 0)

            @constraint(subproblem, generation_capacity_fixed[g in Gs, z in Zs], generation_capacity[g, z].out == generation_capacity[g, z].in)
            @constraint(subproblem, storage_capacity_fixed[s in Ss, z in Zs], storage_capacity[s, z].out == storage_capacity[s, z].in)
            @constraint(subproblem, injection_capacity_fixed[s in Ss, z in Zs], injection_capacity[s, z].out == injection_capacity[s, z].in)
            @constraint(subproblem, extraction_capacity_fixed[s in Ss, z in Zs], extraction_capacity[s, z].out == extraction_capacity[s, z].in)

            @constraint(subproblem, generation_availability_constraint[g in Gs, z in Zs, t in Ts], generation[g, z, t] <= 0)

            @constraint(subproblem, market_clearing[z in Zs, t in Ts],
                sum(generation[g, z, t] for g in Gs) +
                sum(extraction[s, z, t] * model_parameters.discharging_efficiency[s] for s in Ss) +
                sum(flow[z2, z, t] for z2 in Zs if z2 != z) +
                load_shedding[z, t]
                >=
                sum(injection[s, z, t] / model_parameters.charging_efficiency[s] for s in Ss) +
                sum(flow[z, z2, t] for z2 in Zs if z2 != z)
            )

            @constraint(subproblem, injection_capacity_constraint[s in Ss, z in Zs, t in Ts], injection[s, z, t] <= injection_capacity[s, z].in)
            @constraint(subproblem, extraction_capacity_constraint[s in Ss, z in Zs, t in Ts], extraction[s, z, t] <= extraction_capacity[s, z].in)
            @constraint(subproblem, storage_capacity_constraint[s in Ss, z in Zs, t in Ts], storage_level[s, z, t] <= storage_capacity[s, z].in)
            @constraint(subproblem, storage_balance_constraint[s in Ss, z in Zs, t in Ts], storage_level[s, z, t] == storage_level[s, z, previous[t]] + injection[s, z, t] - extraction[s, z, t])

            if local_pricing
                @constraint(subproblem, no_flow[z1 in Zs, z2 in Zs, t in Ts; z1 != z2], flow[z1, z2, t] == 0)
            else
                @constraint(subproblem, flow_capacity_constraint[z1 in Zs, z2 in Zs, t in Ts; z1 != z2], flow[z1, z2, t] <= 100_000)
            end

            @constraint(subproblem, no_self_flow[z in Zs, t in Ts], flow[z, z, t] == 0)

            SDDP.parameterize(subproblem, collect(years)) do weather_year
                generation_availability = generation_availability_by_year[weather_year]
                demand_by_zone_and_time = demand_by_year[weather_year]

                for g in Gs, z in Zs, t in Ts
                    JuMP.set_normalized_coefficient(
                        generation_availability_constraint[g, z, t],
                        generation_capacity[g, z].in,
                        -generation_availability[(g, z, t)],
                    )
                end

                for z in Zs, t in Ts
                    JuMP.set_normalized_rhs(
                        market_clearing[z, t],
                        demand_by_zone_and_time[(z, t)],
                    )
                end
            end

            @stageobjective(
                subproblem,
                sum(model_parameters.generation_marginal_cost_per_mwh[g] * generation[g, z, t] for g in Gs, z in Zs, t in Ts) +
                sum(model_parameters.transmission_flow_cost_per_mwh * flow[z1, z2, t] for z1 in Zs, z2 in Zs, t in Ts if z1 != z2) +
                sum(1e6 * load_shedding[z, t] for z in Zs, t in Ts)
            )
        end
    end

    SDDP.train(stochastic_model; iteration_limit = iteration_limit)

    stochastic_cost_minimization_results = []

    for weather_year in years
        simulations = SDDP.simulate(
            stochastic_model,
            1,
            [
                :generation,
                :injection,
                :extraction,
                :storage_level,
                :flow,
                :generation_capacity,
                :storage_capacity,
                :injection_capacity,
                :extraction_capacity,
            ],
            sampling_scheme = SDDP.Historical([(1, nothing), (2, weather_year)]),
            skip_undefined_variables = true,
        )

        simulation = simulations[1]
        second_stage_result = simulation[2]

        demand_by_zone_and_time = demand_by_year[weather_year]

        demand_array = JuMP.Containers.DenseAxisArray(
            [demand_by_zone_and_time[(z, t)] for z in Zs, t in Ts],
            Zs,
            collect(Ts),
        )

        prices_array = JuMP.Containers.DenseAxisArray(
            zeros(length(Zs), length(Ts)),
            Zs,
            collect(Ts),
        )

        second_stage_result[:demand] = demand_array
        second_stage_result[:prices] = prices_array

        push!(stochastic_cost_minimization_results, (2, second_stage_result))
    end

    @save joinpath(@__DIR__, "results", "stochastic_cost_minimization_results.jld2") stochastic_cost_minimization_results
    return nothing
end


# PT.2.8 – Stochastic Visualization
# ---------------------------------

function create_stochastic_cost_minimization_results_visualization(local_pricing::Bool = true)
    results_path = joinpath(@__DIR__, "results", "stochastic_cost_minimization_results.jld2")
    stochastic_results = load(results_path, "stochastic_cost_minimization_results")

    color_map = Dict(
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

    plot_colors = Dict(k => parse(Makie.Colors.Colorant, v) for (k, v) in color_map)
    zone_colors = [colorant"#1565c0", colorant"#f9a825", colorant"#2e7d32", colorant"#8e24aa"]

    generation_technologies_local = generation_technologies
    storage_technologies_local = storage_technologies
    zones_local = zones
    time_steps_local = time_steps
    x_values = collect(time_steps_local)

    year_to_index = Dict{Int,Int}()
    for (idx, item) in enumerate(stochastic_results)
        year_to_index[item[2][:noise_term]] = idx
    end
    available_years = sort(collect(keys(year_to_index)))

    fig = Figure(size = (1600, 1200))

    pricing_label = local_pricing ? "Local Pricing" : "Network Pricing"
    Label(fig[1, 1], "Stochastic Cost Minimization ($pricing_label)", fontsize = 14, halign = :left)
    dd_year = Menu(fig[1, 2], options = available_years)

    ax_price = Axis(fig[2, 1:4], title = "Prices", ylabel = "Price [€/MWh]", xlabel = "Hour of Year")
    ax_dispatch = [
        Axis(
            fig[2 + i, 1:4],
            title = "Dispatch $(zones_local[i])",
            ylabel = "Generation [MW]",
            xlabel = "Hour of Year",
        )
        for i in 1:4
    ]

    legend_order = [
        "Exports",
        "Storage Injection - Battery",
        "Storage Injection - Hydrogen",
        "Imports",
        "Storage Extraction - Battery",
        "Storage Extraction - Hydrogen",
        "Generation - Wind offshore",
        "Generation - Wind onshore",
        "Generation - Photovoltaics",
        "Generation - Loss of Load",
    ]
    legend_elements = [PolyElement(color = plot_colors[k]) for k in legend_order]
    Legend(fig[3:6, 5], legend_elements, legend_order, framevisible = true, labelsize = 10)

    ax_generation_capacity = [Axis(fig[7, i], title = "$(zones_local[i]) Capacities") for i in 1:4]
    ax_injection_capacity = [Axis(fig[8, i]) for i in 1:4]
    ax_extraction_capacity = [Axis(fig[9, i]) for i in 1:4]
    ax_storage_capacity = [Axis(fig[10, i]) for i in 1:4]

    ax_generation_capacity[1].ylabel = "Gen [MW]"
    ax_injection_capacity[1].ylabel = "Inj [MW]"
    ax_extraction_capacity[1].ylabel = "Ext [MW]"
    ax_storage_capacity[1].ylabel = "Str [MWh]"

    for row in 7:10
        rowsize!(fig.layout, row, Fixed(100))
    end

    all_capacity_axes = vcat(
        ax_generation_capacity,
        ax_injection_capacity,
        ax_extraction_capacity,
        ax_storage_capacity,
    )

    zone_legend_elements = [LineElement(color = zone_colors[iz]) for iz in 1:4]
    Legend(fig[2, 5], zone_legend_elements, zones_local, framevisible = true, labelsize = 10)

    function update_plot(weather_year)
        idx = year_to_index[weather_year]
        result = stochastic_results[idx][2]

        prices = result[:prices]
        generation = result[:generation]
        injection = result[:injection]
        extraction = result[:extraction]
        flow = result[:flow]

        for ax in [ax_price; ax_dispatch; all_capacity_axes]
            empty!(ax)
        end

        for (zone_index, zone) in enumerate(zones_local)
            lines!(
                ax_price,
                x_values,
                [prices[zone, t] for t in time_steps_local],
                color = zone_colors[zone_index],
            )
        end

        for (zone_index, zone) in enumerate(zones_local)
            positive_keys = String[]
            positive_values = Vector{Float64}[]

            for technology in generation_technologies_local
                push!(positive_keys, "Generation - $technology")
                push!(positive_values, [generation[technology, zone, t] for t in time_steps_local])
            end

            for storage_technology in storage_technologies_local
                efficiency = storage_technology == "Battery" ? 0.95 : 0.60
                push!(positive_keys, "Storage Extraction - $storage_technology")
                push!(
                    positive_values,
                    [extraction[storage_technology, zone, t] * efficiency for t in time_steps_local],
                )
            end

            push!(positive_keys, "Imports")
            push!(
                positive_values,
                [sum(flow[z2, zone, t] for z2 in zones_local if z2 != zone) for t in time_steps_local],
            )

            positive_matrix = hcat(positive_values...)
            positive_colors = [plot_colors[k] for k in positive_keys]
            cumulative_positive = cumsum(positive_matrix, dims = 2)

            for j in size(cumulative_positive, 2):-1:1
                upper = cumulative_positive[:, j]
                lower = j > 1 ? cumulative_positive[:, j - 1] : zeros(length(time_steps_local))
                band!(ax_dispatch[zone_index], x_values, lower, upper, color = positive_colors[j])
            end

            negative_data = zeros(length(time_steps_local))
            for storage_technology in storage_technologies_local
                efficiency = storage_technology == "Battery" ? 0.95 : 0.60
                values = [injection[storage_technology, zone, t] / efficiency for t in time_steps_local]
                band!(
                    ax_dispatch[zone_index],
                    x_values,
                    negative_data .- values,
                    negative_data,
                    color = plot_colors["Storage Injection - $storage_technology"],
                )
                negative_data .-= values
            end

            exports = [sum(flow[zone, z2, t] for z2 in zones_local if z2 != zone) for t in time_steps_local]
            band!(
                ax_dispatch[zone_index],
                x_values,
                negative_data .- exports,
                negative_data,
                color = plot_colors["Exports"],
            )
            hlines!(ax_dispatch[zone_index], [0], color = :black, linewidth = 0.5)
        end

        generation_capacity = result[:generation_capacity]
        storage_capacity = result[:storage_capacity]
        injection_capacity = result[:injection_capacity]
        extraction_capacity = result[:extraction_capacity]

        generation_colors = [plot_colors["Generation - $g"] for g in generation_technologies_local]
        injection_colors = [plot_colors["Storage Injection - $s"] for s in storage_technologies_local]
        extraction_colors = [plot_colors["Storage Extraction - $s"] for s in storage_technologies_local]

        for (zone_index, zone) in enumerate(zones_local)
            generation_values = [generation_capacity[g, zone].out for g in generation_technologies_local]
            barplot!(ax_generation_capacity[zone_index], 1:4, generation_values, color = generation_colors)
            ax_generation_capacity[zone_index].xticks = (1:4, ["W.off", "W.on", "PV", "LoL"])
            ax_generation_capacity[zone_index].xticklabelrotation = π / 4

            injection_values = [injection_capacity[s, zone].out for s in storage_technologies_local]
            barplot!(ax_injection_capacity[zone_index], 1:2, injection_values, color = injection_colors)
            ax_injection_capacity[zone_index].xticks = (1:2, ["Bat", "H₂"])
            if maximum(injection_values) == 0
                ylims!(ax_injection_capacity[zone_index], 0, 1)
            end

            extraction_values = [extraction_capacity[s, zone].out for s in storage_technologies_local]
            barplot!(ax_extraction_capacity[zone_index], 1:2, extraction_values, color = extraction_colors)
            ax_extraction_capacity[zone_index].xticks = (1:2, ["Bat", "H₂"])
            if maximum(extraction_values) == 0
                ylims!(ax_extraction_capacity[zone_index], 0, 1)
            end

            storage_values = [storage_capacity[s, zone].out for s in storage_technologies_local]
            barplot!(ax_storage_capacity[zone_index], 1:2, storage_values, color = extraction_colors)
            ax_storage_capacity[zone_index].xticks = (1:2, ["Bat", "H₂"])
            if maximum(storage_values) == 0
                ylims!(ax_storage_capacity[zone_index], 0, 1)
            end
        end
    end

    on(dd_year.selection) do weather_year
        update_plot(weather_year)
    end

    update_plot(available_years[1])
    display(fig)
end