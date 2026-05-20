# =============================================================================
# SPEI Pipeline — Step 2 (Single State)
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
run_spei_pipeline <- function(
    state_safe,
    input_file,
    output_dir
) {

    paste("Starting R script")

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
        paste0("SPEI12_", state_safe, ".tif")
    )

    if (file.exists(out_check)) {
        stop(
            paste(
                "Already processed:",
                state_safe,
                "— delete output files to rerun."
            )
        )
    }

    # =========================================================================
    # SPEI FUNCTION
    #
    # Input:
    #   x = 240-length vector of monthly P-PET values
    #
    # Output:
    #   340-length vector:
    #     [1:240]   SPEI-1
    #     [241:320] SPEI-3 seasonal months only
    #     [321:340] SPEI-12 annual only
    # =========================================================================
    spei_function <- function(x, ...) {

        tryCatch({

            if (all(is.na(x))) {
                return(rep(NA, 340))
            }

            pixel_ts <- ts(
                x,
                start = c(2004, 1),
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

            if (length(spei1_all) != 240) {
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
            annual_idx <- seq(12, 240, by = 12)

            spei12_sel <- spei12_all[annual_idx]

            return(
                c(
                    spei1_all,
                    spei3_sel,
                    spei12_sel
                )
            )

        }, error = function(e) {

            return(rep(NA, 340))

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
            "bands (expected 240)\n"
        )
    )

    # =========================================================================
    # COMPUTE BLOCK BY BLOCK
    # =========================================================================
    cat("Running SPEI computation...\n")

    temp_file <- file.path(
        output_dir,
        paste0(state_safe, "_temp.tif")
    )

    result_brick <- brick(
        p_pet_brick,
        nl = 340
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
        rep(2004:2023, each = 12),
        "_m",
        sprintf("%02d", rep(1:12, 20))
    )

    spei3_names <- paste0(
        "y",
        rep(2004:2023, each = 4),
        "_m",
        sprintf("%02d", rep(c(3, 6, 9, 12), 20))
    )

    spei12_names <- paste0(
        "y",
        2004:2023
    )

    # =========================================================================
    # SPLIT OUTPUTS
    # =========================================================================
    cat("Saving output files...\n")

    all_b <- brick(temp_file)

    spei1_brick  <- all_b[[1:240]]
    spei3_brick  <- all_b[[241:320]]
    spei12_brick <- all_b[[321:340]]

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
            paste0("SPEI1_", state_safe, ".tif")
        ),
        format = "GTiff",
        overwrite = TRUE,
        NAflag = -9999
    )

    writeRaster(
        spei3_brick,
        file.path(
            output_dir,
            paste0("SPEI3_", state_safe, ".tif")
        ),
        format = "GTiff",
        overwrite = TRUE,
        NAflag = -9999
    )

    writeRaster(
        spei12_brick,
        file.path(
            output_dir,
            paste0("SPEI12_", state_safe, ".tif")
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
            state_safe,
            ".tif — ",
            nlayers(spei1_brick),
            " bands\n"
        )
    )

    cat(
        paste0(
            "  SPEI3_",
            state_safe,
            ".tif — ",
            nlayers(spei3_brick),
            " bands\n"
        )
    )

    cat(
        paste0(
            "  SPEI12_",
            state_safe,
            ".tif — ",
            nlayers(spei12_brick),
            " bands\n"
        )
    )
}

# =============================================================================
# FUNCTION CALL
# =============================================================================
run_spei_pipeline(
    state_safe = "Madhya_Pradesh",
    input_file = "data/drought_inputs/Madhya_Pradesh/monthly/ppet/P_PET_Madhya_Pradesh_monthly_multiband.tif",
    output_dir = "data/drought_inputs/Madhya_Pradesh/monthly/ppet"
)