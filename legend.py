from PIL import Image, ImageDraw

def tmp_legend_text(skip_ends=True):
    def legend_text(draw, colormap, segment_height):
        for idx, entry in enumerate(colormap):
            y_adjust = 0
            if (idx == 0):
                if skip_ends:
                    continue
                y_adjust = 15
            if (idx == len(colormap) - 1):
                if skip_ends:
                    continue
                y_adjust = -15

            y_position = int(idx * segment_height) + y_adjust
            celcius = int(entry[0])
            farenheit = int(entry[0]*1.8 + 32)

            draw.text((70, y_position),
                    f"{celcius}°C  /  {farenheit}°F",
                    fill=(0, 0, 0),
                    anchor='mm')

    return legend_text

def prate_legend_text(draw, colormap, segment_height):
    for idx, entry in enumerate(colormap):
        if idx == 0 or idx == len(colormap) - 1:
            continue

        y_position = int(idx * segment_height)
        mm = float(entry[0]) * 3600
        inch = round(mm*3/64, 3)

        draw.text((70, y_position),
                   f"{inch:.2f} in  /  {mm} mm",
                   fill=(0, 0, 0),
                   anchor='mm')

def legend_text(text):
    def legend_text(draw, colormap, segment_height):
        for idx, entry in enumerate(colormap):
            y_adjust = 0
            if (idx == 0):
                y_adjust = 15
            if (idx == len(colormap) - 1):
                y_adjust = -15

            y_position = int(idx * segment_height) + y_adjust
            draw.text((70, y_position),
                    f"{entry[0]}{text}",
                    fill=(0, 0, 0),
                    anchor='mm')
    return legend_text

def pres_legend_text(draw, colormap, segment_height):
    for idx, entry in enumerate(colormap):
        if idx == 0 or idx == len(colormap) - 1:
            continue

        y_position = int(idx * segment_height)
        pressure = int(entry[0]) / 100

        draw.text((70, y_position),
                   f"{pressure:.0f} hPa",
                   fill=(0, 0, 0),
                   anchor='mm')


def build_legend(layer, dest_path):
    # Get the color map
    colormap = layer['color_scale']
    
    # Define the dimensions of the legend image
    legend_width = 120
    legend_height = 300
    color_bar_width = 20
    num_colors = len(colormap)
    segment_height = legend_height / (num_colors - 1)
    
    # Create a new image with a white background
    legend = Image.new('RGB', (legend_width, legend_height), (255, 255, 255))
    draw = ImageDraw.Draw(legend)

    for i in range(legend_height):
        # Determine which segment we're in and the local position within the segment
        segment_index = int(i // segment_height)
        segment_pos = (i % segment_height) / segment_height
        
        # Interpolate between the colors of the current segment
        if segment_index < num_colors - 1:
            start_color = colormap[segment_index]
            end_color = colormap[segment_index + 1]
            color = interpolate_color(start_color, end_color, segment_pos)
            draw.line([(0, i), (color_bar_width, i)], fill=color)

    # Add color band labels
    legend_text = layer['legend_text']
    legend_text(draw, colormap, segment_height)

    # Save the image
    legend.save(dest_path)

def interpolate_color(start_color, end_color, t):
    r = int(start_color[1] + (end_color[1] - start_color[1]) * t)
    g = int(start_color[2] + (end_color[2] - start_color[2]) * t)
    b = int(start_color[3] + (end_color[3] - start_color[3]) * t)
    return (r, g, b)
