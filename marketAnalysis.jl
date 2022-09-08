using FloatingTableView
using Statistics
using Impute 
using CairoMakie
using AlgebraOfGraphics
using Plots 
using StatsBase

include("dataProcessing.jl")

africanExchanges = ["BSE","GSE","JSE","LUSE","MSE","NSE","USE","ZSE","NGX","BRVM"]

fullMarketDate = DataFrame(MarketDate = collect(Dates.Date(2017,12,01):Dates.Day(1):Dates.Date(2022,08,10)))

exchangesMktData = DataFrame(
    MarketDate=Date[],
    Exchange = Union{Missing,String}[],
    Price =  Union{Missing,Float64}[])
    

for exchange in africanExchanges   
    priceData = getProcessedData(exchange)
    ans = leftjoin(fullMarketDate, priceData, on = :MarketDate) |> sort |> Impute.locf
    append!(exchangesMktData,ans)     
end

CSV.write("africanIndexData.csv",exchangesMktData)

#missingDf = describe(exchangesMktData, :nmissing)
#filter([:IndexName , :IndexPrice] => (l, v) -> ismissing(l) && ismissing(v),exchangesMktData)

cleanMarketData = 
    exchangesMktData |> 
    exchangesMktData -> filter(:MarketDate => >=(Date("2018-01-02","y-m-d")),exchangesMktData)|>
    exchangesMktData -> transform(exchangesMktData,:Price .=> ByRow(Float64),renamecols = false) |>
    exchangesMktData -> transform(groupby(exchangesMktData,:Exchange),:MarketDate => :MarketDate,:Price .=> returnFunction => :Returns)

summaryStatistics = combine(groupby(cleanMarketData,:Exchange),:Returns .=> 
                                    [length,minimum,maximum,mean,median,std,skewness,kurtosis] .=> 
                                    [:Nobs,:Min,:Max,:Mean,:Median,:Stdev,:Skewness,:Kurtosis]) 

summaryStatistics = hcat(summaryStatistics[:,[:1,:2]],round.(summaryStatistics[:,:3:end];digits=2))

    
function  generateReturnPlot(cleanMarketData::AbstractDataFrame,Exchange::String,column::String)
        
    pltOutput = Plots.plot(cleanMarketData[:,:MarketDate],
                             cleanMarketData[:,column],
                            #group = cleanMarketData[:,:Exchange],
                            title  = Exchange,
                            linewidth=1.0,
                            size = (900,800),
                            legend = false)
    return pltOutput
end

returnPlotArray = [] 
pricePlotArray = [] 

for (key,value) in pairs(groupby(cleanMarketData,:Exchange))
    push!(returnPlotArray, generateReturnPlot(value,string(key[1]),"Returns"))
    push!(pricePlotArray, generateReturnPlot(value,string(key[1]),"Price"))
end

Plots.plot(returnPlotArray...,
           layout=(5,2),
           size = (1000,1000),
           #plot_yticks = (1:length(labels),labels),
           xlabel = "Date",
           ylabel = "Returns",
           ink=:all,
           plot_title  = "African Index Returns Overtime")
           
Plots.plot(pricePlotArray...,
           layout=(5,2),
           size = (1000,1000),
           #plot_yticks = (1:length(labels),labels),
           xlabel = "Date",
           ylabel = "Price",
           ink=:all,
           plot_title  = "African Index Price Overtime")
             
meanDf = combine(groupby(cleanMarketData,:Exchange),:Returns => mean => :MeanReturn)
meanDf[!,:MeanReturn] = meanDf[:,:MeanReturn] .* 100.0

barplot(meanDf[:,:MeanReturn],
    bar_labels = meanDf[:,:Exchange],
    axis = (title="Average Returns OVertime for the Exchanges",),
    label_size = 12,
    flip_labels_at=(-0.8, 0.8),
    label_offset = 10,
    label_rotation = -90.0 )

##6,1,3,5 year return 
GeometricLink(input::AbstractVector{Float64}) = (reduce(*,  (input .+ 1.0))) - 1.0

function getPeriodicReturns(inputDf::AbstractDataFrame,period::Int)
    refDate = last(inputDf[:,:MarketDate]) - Dates.Month(period)
    newColName = string(string(period) ," ","Month Return")
    res = 
        inputDf |>
        inputDf -> filter(:MarketDate => >=(refDate),inputDf) |>
        inputDf -> combine(inputDf,:Returns => GeometricLink => newColName)
    return res[1,1] 
end

periods = [3,6,12,36,60]
periodicRets = zeros(10,length(periods))  

for (i,gdf) in zip(1:10,groupby(cleanMarketData,:Exchange))
     periodicRets[i,:] = map(x -> (getPeriodicReturns(gdf,x) .* 100.0),periods)
end

ColNames = string.(string.(periods) ," ","Month Return")
periodicReturnsDF = hcat(DataFrame(keys(groupby(cleanMarketData,:Exchange))),DataFrame(periodicRets,ColNames))

 
function generatePlot(df::AbstractDataFrame,col::String)
        
    labels = africanExchanges #string.(df[:,:Exchange])
    barLabels = string.(round.(df[:,col];digits=2))

         pltOutPut = 
         Plots.plot(df[:,col],
                    seriestype = :bar,
                    orientation=:h,
                    yticks = (1:length(labels),labels),
                    yflip = true,
                    size = (600,700),
                    title  = col,
                    legend=false)
                    #xlabel = "Returns",
                    #ylabel = "Exchange",
                    #xlim = (3.0,3.50),
                    #series_annotations = barLabels)
                    #series_annotations = Plots.text.(barLabels,:left,family="serif"))
            
            return pltOutPut
end

x = names(periodicReturnsDF)[2:end][1]
plot_array = [] 
for x in names(periodicReturnsDF)[2:end]
    push!(plot_array, generatePlot(periodicReturnsDF,x))
end

labels = string.(periodicReturnsDF[:,:Exchange])

Plots.plot(plot_array...,
           layout=(1,5),
           size = (1000,800),
           plot_yticks = (1:length(labels),labels),
           xlabel = "Returns",
           plot_ylabel = "Exchange",
           ink=:all,
           plot_title  = "Returns for Major African Indices")

#=
Correlation Among the Indexs
=#

Matrix(cleanMarketData[:,Not(:MarketDate)])

exchangeNames = names(select(unstack(cleanMarketData,:MarketDate,:Exchange,:Returns),Not(:MarketDate)))
corrMat = cor(Matrix(select(unstack(cleanMarketData,:MarketDate,:Exchange,:Returns),Not(:MarketDate))))
Plots.heatmap(africanExchanges,exchangeNames,corrMat, yflip=true,size=(900,600))
















