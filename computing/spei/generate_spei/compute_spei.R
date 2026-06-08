# =============================================================================
# SPEI Pipeline —
# Read multiband P-PET GeoTIFF, compute SPEI-1/3/12 pixel-wise,
# write 3 multiband output GeoTIFFs with named bands.
# =============================================================================

# =============================================================================
# INSTALL (Run once if needed)
# =============================================================================
# install.packages(c("SPEI", "raster"), repos="https://cloud.r-project.org")

# =============================================================================
# LIBRARIES
# =============================================================================
library(SPEI)
library(raster)

# =============================================================================
# MAIN FUNCTION
# =============================================================================
run_spei_pipeline <- function(aez, start_year, end_year) {

    paste("Starting R script")
    paste("aez", aez)
    paste("start_year", start_year)

    input_file = paste0("data/drought_inputs/", aez, "/monthly/P_PET_AEZ_", aez, "_monthly_multiband.tif")
    output_dir = paste0("data/drought_inputs/", aez, "/monthly")

    n_years <- end_year - start_year + 1

    n_monthly  <- n_years * 12
    n_seasonal <- n_years * 4
    n_annual   <- n_years

    n_output <- n_monthly + n_seasonal + n_annual

    # -------------------------------------------------------------------------
    # Create output directory
    # -------------------------------------------------------------------------
    if (!dir.exists(output_dir)) {
        dir.create(output_dir, recursive = TRUE)
    }

    # -------------------------------------------------------------------------
    # Resume check
    # -------------------------------------------------------------------------
    out_check <- file.path(
        output_dir,
        paste0("SPEI12_", aez, ".tif")
    )

    if (file.exists(out_check)) {
        stop(
            paste(
                "Already processed:",
                aez,
                "— delete output files to rerun."
            )
        )
    }

    # =========================================================================
    # SPEI FUNCTION
    # Input:
    #   x = n_monthly-length vector of monthly P-PET values
    # Output:
    #     n_output-length vector:
    #     [1 : n_monthly] SPEI-1
    #     [n_monthly+1 : n_seasonal] SPEI-3 seasonal months only
    #     [n_seasonal+1 : n_annual] SPEI-12 annual only
    # =========================================================================
    spei_function <- function(x, ...) {

        tryCatch({

            if (all(is.na(x))) {
                return(rep(NA, n_output))
            }

            pixel_ts <- ts(
                x,
                start = c(start_year, 1),
                frequency = 12
            )

            spei1_all <- as.vector(
                spei(
                    pixel_ts,
                    1,
                    distribution = "log-Logistic",
                    na.rm = TRUE
                )$fitted
            )

            spei3_all <- as.vector(
                spei(
                    pixel_ts,
                    3,
                    distribution = "log-Logistic",
                    na.rm = TRUE
                )$fitted
            )

            spei12_all <- as.vector(
                spei(
                    pixel_ts,
                    12,
                    distribution = "log-Logistic",
                    na.rm = TRUE
                )$fitted
            )

            if (length(spei1_all) != n_monthly) {
                stop("Incorrect output length.")
            }

            # -----------------------------------------------------------------
            # SPEI-3: keep only Mar, Jun, Sep, Dec
            # -----------------------------------------------------------------
            seasonal_idx <- which(
                ((seq_along(spei3_all) - 1) %% 12 + 1) %in%
                c(3, 6, 9, 12)
            )

            spei3_sel <- spei3_all[seasonal_idx]

            # -----------------------------------------------------------------
            # SPEI-12: keep only December
            # -----------------------------------------------------------------
            annual_idx <- seq(12, n_monthly, by = 12)

            spei12_sel <- spei12_all[annual_idx]

            return(
                c(
                    spei1_all,
                    spei3_sel,
                    spei12_sel
                )
            )

        }, error = function(e) {

            return(rep(NA, n_output))

        })
    }

    # =========================================================================
    # LOAD INPUT
    # =========================================================================
    cat(paste("Loading:", input_file, "\n"))

    p_pet_brick <- brick(input_file)


    cat(
        paste(
            "Loaded",
            nlayers(p_pet_brick),
            "bands (expected",
            n_monthly,
            ")\n"
        )
    )

    # =========================================================================
    # VALIDATE INPUT
    # =========================================================================
    if (nlayers(p_pet_brick) != n_monthly) {
        stop(
            paste(
                "Expected",
                n_monthly,
                "bands but found",
                nlayers(p_pet_brick)
            )
        )
    }

    # =========================================================================
    # COMPUTE BLOCK BY BLOCK
    # =========================================================================
    cat("Running SPEI computation...\n")

    temp_file <- file.path(
        output_dir,
        paste0(aez, "_temp.tif")
    )

    result_brick <- brick(
        p_pet_brick,
        nl = n_output
    )

    result_brick <- writeStart(
        result_brick,
        filename = temp_file,
        overwrite = TRUE
    )

    bs <- blockSize(p_pet_brick)

    for (i in 1:bs$n) {

        v <- getValues(
            p_pet_brick,
            row = bs$row[i],
            nrows = bs$nrows[i]
        )

        res <- t(
            apply(v, 1, spei_function)
        )

        writeValues(
            result_brick,
            res,
            bs$row[i]
        )

        cat(
            paste(
                "Chunk",
                i,
                "/",
                bs$n,
                "\n"
            )
        )
    }

    result_brick <- writeStop(result_brick)

    cat("Computation complete.\n")

    # =========================================================================
    # GENERATE BAND NAMES
    # =========================================================================
    spei1_names <- paste0(
        "y",
        rep(start_year:end_year, each = 12),
        "_m",
        sprintf("%02d", rep(1:12, n_years))
    )

    spei3_names <- paste0(
        "y",
        rep(start_year:end_year, each = 4),
        "_m",
        sprintf("%02d", rep(c(3, 6, 9, 12), n_years))
    )

    spei12_names <- paste0(
        "y",
        start_year:end_year
    )

    # =========================================================================
    # SPLIT OUTPUTS
    # =========================================================================
    cat("Saving output files...\n")

    all_b <- brick(temp_file)

    #     spei1_brick  <- all_b[[1:240]]
    #     spei3_brick  <- all_b[[241:320]]
    #     spei12_brick <- all_b[[321:340]]

    spei1_end <- n_monthly
    spei3_end <- n_monthly + n_seasonal

    spei1_brick  <- all_b[[1:spei1_end]]
    spei3_brick  <- all_b[[(spei1_end + 1):spei3_end]]
    spei12_brick <- all_b[[(spei3_end + 1):n_output]]

    names(spei1_brick)  <- spei1_names
    names(spei3_brick)  <- spei3_names
    names(spei12_brick) <- spei12_names

    # =========================================================================
    # WRITE OUTPUTS
    # =========================================================================
    writeRaster(
        spei1_brick,
        file.path(
            output_dir,
            paste0("SPEI1_", aez, ".tif")
        ),
        format = "GTiff",
        overwrite = TRUE,
        NAflag = -9999
    )

    writeRaster(
        spei3_brick,
        file.path(
            output_dir,
            paste0("SPEI3_", aez, ".tif")
        ),
        format = "GTiff",
        overwrite = TRUE,
        NAflag = -9999
    )

    writeRaster(
        spei12_brick,
        file.path(
            output_dir,
            paste0("SPEI12_", aez, ".tif")
        ),
        format = "GTiff",
        overwrite = TRUE,
        NAflag = -9999
    )

    # =========================================================================
    # CLEANUP
    # =========================================================================
    file.remove(temp_file)

    # =========================================================================
    # DONE
    # =========================================================================
    cat(
        paste0(
            "\n✅ Done. Output files saved to: ",
            output_dir,
            "\n"
        )
    )

    cat(
        paste0(
            "  SPEI1_",
            aez,
            ".tif — ",
            nlayers(spei1_brick),
            " bands\n"
        )
    )

    cat(
        paste0(
            "  SPEI3_",
            aez,
            ".tif — ",
            nlayers(spei3_brick),
            " bands\n"
        )
    )

    cat(
        paste0(
            "  SPEI12_",
            aez,
            ".tif — ",
            nlayers(spei12_brick),
            " bands\n"
        )
    )
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