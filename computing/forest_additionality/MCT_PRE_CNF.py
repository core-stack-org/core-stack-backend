from .ModelEval import ModelEvaluation

# from MapChecker import MapChecker


class MCT_PRE_CNF:
    def __init__(
        self,
        working_directory,
        predicted_density_cnf,
        mask,
        deforestation_cnf,
        deforestation_cal,
        forest_area_cal,
        grid_area=10000,
    ):
        self.working_directory = working_directory
        self.predicted_density_cnf = predicted_density_cnf
        self.mask = mask
        self.deforestation_cnf = deforestation_cnf
        self.deforestation_cal = deforestation_cal
        self.forest_area_cal = forest_area_cal
        self.grid_area = grid_area
        self.model_evaluation = ModelEvaluation()

        self.title = "Model Evaluation for CNF"
        self.out_fn = "Plot_CNF.png"
        self.out_fn_def = "Acre_Def_Review_CNF.tif"
        self.raster_fn = "Acre_Residuals_CNF.tif"

    def process_data(self):

        images = [
            self.mask,
            self.forest_area_cal,
            self.deforestation_cnf,
            self.deforestation_cnf,
            self.predicted_density_cnf,
        ]
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
        xmax = "Default"
        ymax = "Default"

        self.model_evaluation.set_working_directory(self.working_directory)
        self.model_evaluation.create_mask_polygon(self.mask)
        clipped_gdf = self.model_evaluation.create_thiessen_polygon(
            grid_area,
            self.mask,
            self.predicted_density_cnf,
            self.deforestation_cnf,
            self.out_fn,
            self.raster_fn,
        )
        self.model_evaluation.replace_ref_system(self.mask, self.raster_fn)
        self.model_evaluation.create_deforestation_map(
            self.forest_area_cal,
            self.deforestation_cal,
            self.deforestation_cnf,
            self.out_fn_def,
        )
        self.model_evaluation.replace_legend(self.out_fn_def)
        self.model_evaluation.create_plot(
            self.grid_area, clipped_gdf, self.title, self.out_fn, xmax, ymax
        )
