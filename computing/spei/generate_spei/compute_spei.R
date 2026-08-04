
# SPEI Pipeline
# Read multiband P-PET GeoTIFF, compute SPEI-1/3/12 pixel-wise,
# write 3 multiband output GeoTIFFs with named bands.
# Here the reference baseline period is taken as 2004-2023.
# Change the end_year variable to whatever year you wanna extend the pipeline to.
# If it is not intentional, don't touch the ref_start and ref_end variables for
# extending the pipeline as it will change the SPEI values for all previous years too.
# Run below commands for installing the dependencies if not already installed:
# sudo apt install r-base-core
# conda install -c conda-forge r-spei r-raster
# Rscript -e "install.packages('terra', repos='https://cloud.r-project.org')"
# Rscript -e "install.packages('raster', repos='https://cloud.r-project.org')"

library(SPEI)
library(raster)
library(terra)

run_spei_pipeline <- function(aez, start_year, end_year) {

    input_file <- paste0("data/base_layers/spei/inputs/", aez, "/monthly/P_PET_AEZ_", aez, "_monthly_multiband.tif")
    # output_dir <- paste0("data/base_layers/spei/outputs")

    # if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

    output_base <- "data/base_layers/spei/outputs"

    output_dir1  <- file.path(output_base, "SPEI_1")
    output_dir3  <- file.path(output_base, "SPEI_3")
    output_dir12 <- file.path(output_base, "SPEI_12")

    dir.create(output_dir1, recursive = TRUE, showWarnings = FALSE)
    dir.create(output_dir3, recursive = TRUE, showWarnings = FALSE)
    dir.create(output_dir12, recursive = TRUE, showWarnings = FALSE)

    # --- YEAR RANGE ---
    ref_start  <- 2004   # baseline period start — distribution fitted on this range
    ref_end    <- 2023   # baseline period end   — freeze this when extending to future years

    n_years    <- end_year - start_year + 1
    n_monthly  <- n_years * 12
    n_seasonal <- n_years * 4
    n_annual   <- n_years
    n_output   <- n_monthly + n_seasonal + n_annual

    # --- Resume check ---
    # out_check <- file.path(output_dir, paste0("SPEI12_", aez, ".tif"))
    # if (file.exists(out_check)) {
    #   stop(paste("Already processed:", aez, "— delete output files to rerun."))
    # }

    # =============================================================================
    # SPEI FUNCTION — do not modify
    # Input:  variable-length P-PET time series
    # Output: variable-length vector (SPEI-1 all: 12*(no. of years), SPEI-3 seasonal: 4*(no. of years), SPEI-12 annual: no. of years)
    # =============================================================================
    spei_function <- function(x, ...) {
      tryCatch({
        if (all(is.na(x))) return(rep(NA, n_output))

        pixel_ts <- ts(x, start = c(start_year, 1), frequency = 12)

        spei1_all  <- as.vector(spei(pixel_ts, 1,
                        distribution = 'log-Logistic',
                        ref.start = c(ref_start, 1),
                        ref.end   = c(ref_end, 12),
                        na.rm = TRUE)$fitted)
        spei3_all  <- as.vector(spei(pixel_ts, 3,
                        distribution = 'log-Logistic',
                        ref.start = c(ref_start, 1),
                        ref.end   = c(ref_end, 12),
                        na.rm = TRUE)$fitted)
        spei12_all <- as.vector(spei(pixel_ts, 12,
                        distribution = 'log-Logistic',
                        ref.start = c(ref_start, 1),
                        ref.end   = c(ref_end, 12),
                        na.rm = TRUE)$fitted)

        if (length(spei1_all) != n_monthly) stop("Incorrect output length.")

        seasonal_idx <- which(((seq_along(spei3_all) - 1) %% 12 + 1) %in% c(3,6,9,12))
        annual_idx   <- seq(12, n_monthly, by = 12)

        c(spei1_all, spei3_all[seasonal_idx], spei12_all[annual_idx])

      }, error = function(e) rep(NA, n_output))
    }

    # --- Load input ---
    cat(paste("Loading:", input_file, "\n"))
    p_pet_brick <- brick(input_file)
    cat(paste("Loaded", nlayers(p_pet_brick), "bands (expected", n_monthly, ")\n"))

    # --- Band names ---
    agri_months <- c(7:12, 1:6)

    years <- unlist(lapply(start_year:end_year, function(y) {
      c(rep(y, 6), rep(y + 1, 6))
    }))

    spei1_names <- paste0(
      "y",
      years,
      "_m",
      sprintf("%02d", rep(agri_months, n_years))
    )

    agri_quarter_months <- c("07_09", "10_12", "01_03", "04_06")

    years3 <- unlist(lapply(start_year:end_year, function(y) {
      c(y, y, y + 1, y + 1)
    }))

    spei3_names <- paste0(
      "y",
      years3,
      "_m",
      sprintf("%s", rep(agri_quarter_months, n_years))
    )

    spei12_names <- paste0(
        start_year:end_year,
        "_",
        (start_year + 1):(end_year + 1)
    )

    # --- Compute block by block ---
    cat("Running SPEI computation...\n")
    temp_file    <- file.path(output_base, paste0(aez, "_temp.tif"))
    result_brick <- brick(p_pet_brick, nl = n_output)
    result_brick <- writeStart(result_brick, filename = temp_file, overwrite = TRUE)

    bs <- blockSize(p_pet_brick)
    for (i in 1:bs$n) {
      v   <- getValues(p_pet_brick, row = bs$row[i], nrows = bs$nrows[i])
      res <- t(apply(v, 1, spei_function))
      writeValues(result_brick, res, bs$row[i])
      if (i %% 5 == 0) cat(paste("  Chunk", i, "/", bs$n, "\n"))
    }
    result_brick <- writeStop(result_brick)
    cat("Computation complete.\n")

    # --- Split and save ---
    cat("Saving output files...\n")
    all_b <- brick(temp_file)

    spei1_end <- n_monthly
    spei3_end <- n_monthly + n_seasonal

    spei1_b  <- all_b[[1:spei1_end]]
    spei3_b  <- all_b[[(spei1_end + 1):spei3_end]]
    spei12_b <- all_b[[(spei3_end + 1):n_output]]

    names(spei1_b)  <- spei1_names
    names(spei3_b)  <- spei3_names
    names(spei12_b) <- spei12_names

    spei1_file  <- file.path(output_dir1,  paste0("SPEI1_",  aez, ".tif"))
    spei3_file  <- file.path(output_dir3,  paste0("SPEI3_",  aez, ".tif"))
    spei12_file <- file.path(output_dir12, paste0("SPEI12_", aez, ".tif"))

    if (file.exists(spei1_file))  file.remove(spei1_file)
    if (file.exists(spei3_file))  file.remove(spei3_file)
    if (file.exists(spei12_file)) file.remove(spei12_file)

    spei1_t <- rast(spei1_b)
    names(spei1_t) <- spei1_names

    terra::writeRaster(
        spei1_t,
        spei1_file,
        overwrite = TRUE,
        NAflag = -9999,
        gdal = c("COMPRESS=LZW")
    )

    r <- terra::rast(spei1_file)

    spei3_t <- rast(spei3_b)
    names(spei3_t) <- spei3_names

    terra::writeRaster(
        spei3_t,
        spei3_file,
        overwrite = TRUE,
        NAflag = -9999
    )

    spei12_t <- rast(spei12_b)
    names(spei12_t) <- spei12_names

    terra::writeRaster(
        spei12_t,
        spei12_file,
        overwrite = TRUE,
        NAflag = -9999
    )

    file.remove(temp_file)

    cat("\nDone.\n")
    cat(paste0("SPEI-1  : ", spei1_file, "\n"))
    cat(paste0("SPEI-3  : ", spei3_file, "\n"))
    cat(paste0("SPEI-12 : ", spei12_file, "\n"))

    cat(paste0("  SPEI1_",  aez, ".tif  — ", nlayers(spei1_b),  " bands\n"))
    cat(paste0("  SPEI3_",  aez, ".tif  — ", nlayers(spei3_b),  " bands\n"))
    cat(paste0("  SPEI12_", aez, ".tif  — ", nlayers(spei12_b), " bands\n"))
}


# =============================================================================
# FUNCTION CALL
# =============================================================================
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
    stop("AEZ argument is required")
}

aez <- args[1]
start_year <- as.integer(args[2])
end_year <- as.integer(args[3])

run_spei_pipeline(aez, start_year, end_year)