import altair as alt
alt.data_transformers.enable("vegafusion")


def get_hist_chart(df):
    click = alt.selection_point(encodings=['x'], on='click', empty=True)

    chart = alt.Chart(df).mark_bar().encode(
        # 'O' treats numbers as distinct categories (1, 2, 3...) rather than a continuous range
        x=alt.X('total_tests_in_session:O', title='Total Tests in Session',
                ),
        
        # The Y axis counts how many rows exist for each X value
        y=alt.Y('count():Q', title='Number of sessions'),
        
        # 1. INTERACTIVITY: Color changes based on selection
        # If a bar is clicked, color it steelblue; otherwise, make it light gray
        color=alt.condition(
            click, 
            alt.value('steelblue'), 
            alt.value('lightgray')
        ),
        
        # 2. TOOLTIPS: Show exact numbers when hovering
        tooltip=[
            alt.Tooltip('total_tests_in_session:O', title='Tests'),
            alt.Tooltip('count():Q', title='Session count')
        ]
    ).properties(
        width=600,
        height=300,
        title='Interactive count of sessions by total tests'
    ).add_params(
        # Add the click selection to the chart
        click
    ).interactive() # 3. PAN/ZOOM: Enables scrolling and dragging

    return chart


def get_hist_chart2(df):
    brush = alt.selection_interval(encodings=['x'])

    chart = alt.Chart(df).transform_density(
        # 2. TRANSFORMATION: Calculate density instead of binning
        'time_since_last_session_seconds',
        as_=['time_since_last_session_seconds', 'density'],
        extent=[0, df['time_since_last_session_seconds'].quantile(0.80)], # Optional: Limit range to ignore outliers
        bandwidth=1000  # Adjust this to smooth the curve (similar to maxbins)
    ).mark_area(color='steelblue', opacity=0.5).encode( # 3. MARK: Use area instead of bar
        
        # X-AXIS: Continuous field (no binning)
        x=alt.X('time_since_last_session_seconds:Q', 
                title='Time since last session (sec)',
                #scale=alt.Scale(
                #        domain=(0, 500000), # Manually set a reasonable range
                #        clamp=True       # Forces points > 500 to be drawn at the edge
                #    )
                ),
        
        # Y-AXIS: The calculated density field
        y=alt.Y('density:Q', title='Density'),
        
        # INTERACTIVITY: Highlight selected range
        color=alt.condition(
            brush, 
            alt.value('steelblue'), 
            alt.value('lightgray')
        ),
        
        # TOOLTIPS: Show value and density
        tooltip=[
            alt.Tooltip('time_since_last_session_seconds:Q', title='Time (s)'),
            alt.Tooltip('density:Q', title='Density', format='.4f')
        ]
    ).properties(
        width=600,
        height=300,
        title='Density plot of time since last session'
    ).add_params(
        brush
    ).interactive() 

    return chart



def get_line(df, case):
    # Define the selection (click on legend or line)

    match case:
        case "Download":
            y_plot = "avg_downstream_mbps"
        case "Upload":
            y_plot = "avg_upstream_mbps"

    highlight = alt.selection_point(
        fields=["connection_periphery"], 
        bind="legend",
        on="click",
        empty=True # 'False' means nothing selected = dimmed. 'True' means all visible.
    )

    # 1. Define the Base Encoding (to avoid repetition)
    # We define the common x, color, and selection logic here.
    base = alt.Chart(df).encode(
        x=alt.X('measurement_date:T', title='Measurement date'),
        color=alt.Color(
            "connection_periphery:N", 
            legend=alt.Legend(
                title="Click to isolate periphery",
                orient="bottom"
            ), 
            scale=alt.Scale(scheme="tableau10"),
        )
    )

    line = base.mark_line().transform_window(
        rolling_mean=f'mean({y_plot})',
        frame=[-15, 15],
        groupby=['connection_periphery']
    ).encode(
        y=alt.Y('rolling_mean:Q', title='Stream speed (MB/s)'),
        opacity=alt.condition(
            highlight, 
            alt.value(1.0), 
            alt.value(0.2)   
        ),
        tooltip=[
            alt.Tooltip("connection_periphery", title="Periphery"),
            alt.Tooltip("rolling_mean:Q", title="Rolling average Mb/s", format=".2f"), # Show the calculated value
            alt.Tooltip("p50_downstream_mbps", title="Raw downstream Mb/s"),
            #alt.Tooltip("p50_upstream_mbps", title="Raw Upstream Mb/s"),
            alt.Tooltip("measurement_date", title="Date"),
        ],
    )

    # 4. Layer them together
    # We add the selection to the final combined chart
    chart = (line).add_selection(highlight).interactive().properties(
            title={
                "text": "",
                "color": "gray",
            },
            height=500,
        )

    return chart

def get_tests(df):
    # 1. Modern Selection Syntax (Altair 5+)
    # 'selection_multi' is deprecated; use 'selection_point'.
    # 'on'='click' allows clicking the bars, 'bind'='legend' keeps legend clicking.
    highlight = alt.selection_point(
        fields=["connection_periphery"], 
        bind="legend",
        on="click",
        empty=True # 'False' means nothing selected = dimmed. 'True' means all visible.
    )

    chart = alt.Chart(df).mark_bar().encode(
        # 2. Explicit Axis Titles
        # Use :O for Ordinal (discrete time steps) or :T for Temporal (continuous time)
        x = alt.X('measurement_date:T', title="Measurement date"),
        y = alt.Y('total_tests:Q', title="Total tests"),
        
        # 3. Improved Color Encoding
        color=alt.Color(
            "connection_periphery:N", 
            legend=alt.Legend(
                title="Click to isolate periphery", # Clearer legend title
                orient="bottom",
                labelLimit=200 # Prevents labels from being cut off
            ), 
            scale=alt.Scale(scheme="tableau10"),
        ),
        
        # 4. Tooltips
        # Stacked bars make comparing absolute values hard; tooltips fix this.
        tooltip=[
            alt.Tooltip('measurement_date:O', title='Date'),
            alt.Tooltip('connection_periphery:N', title='Periphery'),
            alt.Tooltip('total_tests:Q', title='Count', format=',')
        ],

        # 5. Robust Opacity Logic
        opacity=alt.condition(
            highlight, 
            alt.value(1.0), 
            alt.value(0.2)   
        ),
    ).add_params(highlight
    ).properties(
        # 6. Explicit Sizing and Title
        width=600,
        height=400,
        title="Total speed tests"
    )

    return chart

def get_marks(df):
    # 1. Interactive Selection (Keep your existing logic, it's great)
    highlight = alt.selection_point(
        fields=["connection_periphery"], 
        bind="legend",
        on="click",
        empty=True # 'False' means nothing selected = dimmed. 'True' means all visible.
    )

    
    # 2. Define interactive brush for zooming/panning
    brush = alt.selection_interval(encodings=['x', 'y'])

    chart = alt.Chart(df).mark_circle().encode(
        # Use Log scale for RTT to handle outliers gracefully
        x=alt.X('avg_rtt_msec:Q', 
                title='RTT (msec)', 
                scale=alt.Scale(
                    domain=(0, 500), # Manually set a reasonable range
                    clamp=True       # Forces points > 500 to be drawn at the edge
                )
            ),
        
        # Use Linear or Symlog for Loss (if many 0s, linear is fine)
        y=alt.Y('avg_loss_percentage:Q', 
                title='Loss %'),
        
        color=alt.Color(
            "connection_periphery:N", 
            legend=alt.Legend(
                title="Click to isolate periphery",
                orient="bottom"
            ), # Better title
            scale=alt.Scale(scheme="tableau10"),
        ),
        
        # Tooltip: vital for exploring specific outliers
        tooltip=[
            alt.Tooltip('connection_periphery:N', title='Road'),
            alt.Tooltip('avg_rtt_msec:Q', title='RTT (ms)'),
            alt.Tooltip('avg_loss_percentage:Q', title='Loss %'),
            # Add any other identifier column here, e.g., 'timestamp'
        ],
        
        opacity=alt.condition(
            highlight, 
            alt.value(0.8), 
            alt.value(0.05)   
        ),
        # Make points larger only when hovered
        size=alt.condition(~highlight, alt.value(20), alt.value(40))
    ).add_params(
        highlight, 
        brush
    ).properties(
        width=700,
        height=400,
        title="Network performance: RTT vs loss"
    ).interactive()

    return chart