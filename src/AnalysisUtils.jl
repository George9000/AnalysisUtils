module AnalysisUtils
using DataSkimmer, DataFrames, CSV, MethodChains
export header, append_output, surveydf, dfuniqmissing, colvaluecategories, describeDF, pageddf, peekfile

"""
    header(header; sep="-", pre="", post="")

Print header with a line of characters uses as separators from content below.
May add a prefix and postfix, for example, newlines.
Intended as an internal function for use with output functions.
"""
function header(header; sep="-", pre="", post="")
    seplen = length(header)
    println(pre, repeat(sep, seplen), "\n", header, "\n", repeat(sep, seplen), post)
end

"""
    append_output(f::Function, filename::AbstractString; mode::AbstractString="a", prepend = "", append = "")

Take output from a function and write or append it to a text file.
The output is whatever is written to the stdout by the function, with a newline before and after the output.
Positional Args:
f function which will output. This can be added in do format after the append_output call.
filename
mode
prepend, string to place on the newline before the output; default is empty string
append, string to place on the newline after the output; default is empty string
"""
function append_output(f::Function, filename::AbstractString;
    mode::AbstractString="a", prepend = "", append = "")
    open(filename, mode) do io
        redirect_stdout(io) do
            println(prepend)
            f()
            println(append)
        end
    end
end

"""
    surveydf(df, symb::Tuple = ())

Run describe and skim on a dataframe.
Show unique categorical values of symbols in the tuple. a tuple of tuples: ((symbol, n_unique_values),).

Positional Arguments:
df
symb, a tuple of tuples ((symbol, n_unique_values),)
  default is an empty tuple
"""
function surveydf(df::DataFrame, symb::Tuple = ())
    show(describe(df, :min, :max, :nmissing, :nuniqueall, :eltype), allcols = true, allrows = true, truncate = 0)
    println("\n")
    show(skim(df))
    println("\n")
    if length(symb) > 0
        for item in symb
            s, n = item
            n = min(length(unique(df[:,s])), n)
            header("Top " * string(n) * " unique values of " * String(s), sep = "-")
            g = groupby(df, s)
            r = combine(g, nrow, proprow => "proportion")
            r = transform!(r, :proportion => ByRow(p -> 100*p), renamecols = false)
            r = sort(r, :nrow, rev = true)
            r = r[1:n, :]
            show(r, allrows = true, truncate = 0)
            println("\n")
        end
    end
end

"""
    describeDF(df, name, cats)

Run the function, surveydf, on a df preceded by a header string followed by a line of separator characters.

Positional Arguments:
df, symbol for dataframe
name, string for a header title
cats, tuple, a tuple of tuples of categorical columns ((symbol, n_unique_values),)
"""
function describeDF(df, name, cats)
   header(name, sep = "=")
   println("\n")
   surveydf(df, cats)
end

"""
    dfuniqmissing(df::DataFrame)

Describe a dataframe's unique and missing values
"""
function dfuniqmissing(df)
    nr = nrow(df)
    @mc describe(df, :eltype, :nuniqueall, :nmissing, :min, :max).{
        transform(it, :nmissing => ByRow(n -> n / nr * 100) => :percent_missing)
        transform(it, :nuniqueall => ByRow(n -> n / nr * 100) => :percent_unique)
        select(it, Not(:min, :max), :min, :max)
        show(it, truncate = 0, allcols = true, allrows = true)
    }
end

"""
    colvaluecategories(df::DataFrame, f::Function, col::Symbol, newcol::Symbol)

Using f to categorize a column's values,
give the nrows and percent of values in that category.
"""
function colvaluecategories(df::DataFrame, f::Function, col::Symbol, newcol::Symbol)
    @mc df.{
        nrows = nrow(it)
        select(it, col)
        transform(it, col => ByRow(f) => newcol)
        groupby(it, newcol)
        combine(it, nrow)
        sort(it, :nrow, rev = true)
        transform(it, :nrow => ByRow(nr -> round((nr / nrows) * 100,
                                                 digits = 2)) => :percent)
        show(it)}
    println("\n\n")
    @mc df.{
        select(it, col)
        transform(it, col => ByRow(f) => newcol)
        subset(it, newcol => ByRow(nc -> !(nc)), skipmissing = true)
        unique(it, col)
        show(it)}
end

"""
    pageddf(df, rstart, rc, ndisplay)

Function to inspect a dataframe and show a limited number of rows
and a 'paged' view of columns.
Positional args:
df
rstart: start of row count
rc: n of rows to show
ndisplay: columns to show on each line
"""
function pageddf(df, rstart, rc, ndisplay)
    stepdelta= ndisplay - 1
    cdf = ncol(df)
    steps = collect(1:ndisplay:cdf)
    laststep = steps[end]
    rend = rstart + (rc - 1)
    println("\n", "****** DataFrame with ","$(nrow(df)) rows ", "and ", "$cdf columns.", " ******")
    println("\n", "********** Showing ","rows ", "$rstart to", "$rend", " ********** \n")
    if mod(cdf, ndisplay) ≠ 0
        steps = steps[1:end-1]
        for s in steps
            println("Cols ", "$s to ", "$(s + stepdelta)")
            show((@view df[rstart:rend, s:(s + stepdelta)]), allcols = true, allrows = true, truncate = 0)
            println("\n")
        end
        println("Cols ", "$laststep to ", "$cdf")
        show((@view df[rstart:rend, laststep:cdf]), allcols = true, allrows = true, truncate = 0)
        println("\n")
    else
        for s in steps
            println("Cols ", "$s to ", "$(s + stepdelta)")
            show((@view df[rstart:rend, s:(s + stepdelta)]), allcols = true, allrows = true, truncate = 0)
            println("\n")
        end
    end
    return nothing
end

"""
    peekfile(inputpath, filename; n = 10, rev = false)

Look at the first few lines in a text file.
Keywords: n = 10, number of lines; rev = false, list from end of file
"""
function peekfile(inputpath, filename; n = 10, rev = false)
    if rev == true
        for (i, line) in enumerate(Iterators.reverse(eachline(joinpath(inputpath, filename))))
            if i > n
                break
            end
            println(i, "  ", line)
        end
    else
        for (i, line) in enumerate(eachline(joinpath(inputpath, filename)))
            if i > n
                break
            end
            println(i, "  ", line)
        end
    end
end

end # module AnalysisUtils
