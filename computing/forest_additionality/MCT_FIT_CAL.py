from .ModelEval import ModelEvaluation
from .MapChecker import MapChecker


class MCT_FIT_CAL:
    """Model Evaluation Tool for the Calibration Period"""

    def __init__(
        self,
        working_directory,
        fitted_density_CAL,
        mask,
        deforestation_cal,
        plot_title,
        grid_area=10000,
    ):
        self.working_directory = working_directory
        self.density = fitted_density_CAL
        self.deforestation_cal = deforestation_cal
        self.mask = mask
        self.grid_area = grid_area

        self.model_evaluation = ModelEvaluation()
        self.map_checker = MapChecker()

        self.plot_title = plot_title
        self.output_plot_filename = "Plot_CAL.png"
        self.output_residual_image = "Acre_Residuals_CAL.tif"
        self.x_max = "Default"
        self.y_max = "Default"

    def process_data(self):
        images = [self.mask, self.deforestation_cal, self.density]

        # Check if all images have the same resolution
        resolutions = [self.map_checker.get_image_resolution(img) for img in images]
        if len(set(resolutions)) != 1:
            raise ValueError("All images must have the same resolution.")

        # Check if all images have the same number of rows and columns
        dimensions = [self.map_checker.get_image_dimensions(img) for img in images]
        if len(set(dimensions)) != 1:
            raise ValueError(
                "All images must have the same number of rows and columns."
            )

        grid_area = self.grid_area

        xmax = self.x_max
        ymax = self.y_max

        self.model_evaluation.set_working_directory(directory=self.working_directory)
        self.model_evaluation.create_mask_polygon(self.mask)
        clipped_gdf = self.model_evaluation.create_thiessen_polygon(
            self.grid_area,
            self.mask,
            self.density,
            self.deforestation_cal,
            self.output_plot_filename,
            self.output_residual_image,
        )
        self.model_evaluation.replace_ref_system(self.mask, self.output_residual_image)
        self.model_evaluation.create_plot(
            self.grid_area,
            clipped_gdf,
            self.plot_title,
            self.output_plot_filename,
            xmax,
            ymax,
        )
        self.model_evaluation.remove_temp_files()
