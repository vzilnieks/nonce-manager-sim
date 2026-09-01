library(jsonlite)
library(readr)
library(purrr)
library(dplyr)
library(tidyr)
library(ggplot2)

args <- commandArgs(trailingOnly = TRUE)
supported_args <- "--brief"
unknown_args <- setdiff(args, supported_args)
if (length(unknown_args) > 0) {
  stop(paste("Unknown option(s):", paste(unknown_args, collapse = ", ")))
}
brief <- "--brief" %in% args

files <- list.files("data", full.names = TRUE, pattern = "\\.jsonl$")
if (length(files) == 0) {
  stop("No JSONL benchmark results found in data/")
}

read_jsonl_safe <- function(path) {
  lines <- readr::read_lines(path, progress = FALSE)
  parsed <- map(lines[nzchar(lines)], ~ tryCatch(
    jsonlite::fromJSON(.x, simplifyVector = TRUE),
    error = function(e) NULL
  ))
  parsed <- compact(parsed)
  if (length(parsed) == 0) {
    return(tibble())
  }
  bind_rows(parsed) %>% mutate(source_file = basename(path))
}

df <- map_dfr(files, read_jsonl_safe)
if (nrow(df) == 0) {
  stop("The JSONL files in data/ did not contain valid result rows")
}

required <- c(
  "manager", "fault", "run_id", "gomaxprocs", "clients", "qps", "addrs",
  "reorg", "duration_seconds", "warmup_seconds", "max_delta", "cap_bits",
  "am_window", "p50", "p95", "p99", "p999", "operation_p50",
  "operation_p95", "operation_p99", "operation_p999", "revert_p50",
  "revert_p95", "revert_p99", "revert_p999", "reverts", "achieved_qps", "valid"
)
missing <- setdiff(required, names(df))
if (length(missing) > 0) {
  stop(paste("Missing required result fields:", paste(missing, collapse = ", ")))
}

invalid <- df %>% filter(is.na(valid) | !valid)
if (nrow(invalid) > 0) {
  warning(sprintf("Ignoring %d invalid benchmark result(s)", nrow(invalid)))
}

df <- df %>%
  filter(valid) %>%
  mutate(
    manager = factor(manager, levels = c("aa", "am", "mutex")),
    fault = as.numeric(fault),
    fault_label = factor(sprintf("%.2f", fault)),
    reorg = as.logical(reorg),
    across(c(p50, p95, p99, achieved_qps), as.numeric)
  )

if (nrow(df) == 0) {
  stop("No valid benchmark results remain after validation")
}

if ("p999" %in% names(df)) {
  df <- df %>% mutate(p999 = as.numeric(p999))
  percentile_columns <- c("p50", "p95", "p99", "p999")
} else {
  percentile_columns <- c("p50", "p95", "p99")
}

dir.create("assets", showWarnings = FALSE, recursive = TRUE)
scenario_keys <- c(
  "gomaxprocs", "clients", "qps", "addrs", "reorg", "duration_seconds",
  "warmup_seconds", "max_delta", "cap_bits", "am_window"
)
scenarios <- df %>%
  group_by(across(all_of(scenario_keys))) %>%
  group_split()
brief_figure_blocks <- character()

for (scenario in scenarios) {
  values <- scenario %>% slice(1)
  suffix <- sprintf(
    "cpu-%s_clients-%s_qps-%s_addrs-%s_reorg-%s_warmup-%ss_duration-%ss_cap-%s_amwin-%s_delta-%s",
    values$gomaxprocs,
    values$clients,
    values$qps,
    values$addrs,
    tolower(as.character(values$reorg)),
    values$warmup_seconds,
    values$duration_seconds,
    values$cap_bits,
    values$am_window,
    values$max_delta
  )

  allocation_latency <- scenario %>%
    select(manager, fault_label, all_of(percentile_columns)) %>%
    mutate(metric = "Allocate")

  operation_latency <- scenario %>%
    select(
      manager,
      fault_label,
      all_of(paste0("operation_", percentile_columns))
    ) %>%
    rename_with(~sub("^operation_", "", .x), starts_with("operation_")) %>%
    mutate(metric = "Allocate + optional Revert")

  revert_latency <- scenario %>%
    filter(reverts > 0) %>%
    select(
      manager,
      fault_label,
      all_of(paste0("revert_", percentile_columns))
    ) %>%
    rename_with(~sub("^revert_", "", .x), starts_with("revert_")) %>%
    mutate(metric = "Revert")

  latency_sources <- if (brief) {
    list(operation_latency)
  } else {
    list(allocation_latency, operation_latency, revert_latency)
  }

  latency <- bind_rows(latency_sources) %>%
    pivot_longer(
      all_of(percentile_columns),
      names_to = "percentile",
      values_to = "latency_seconds"
    ) %>%
    filter(!is.na(latency_seconds)) %>%
    mutate(
      percentile = factor(percentile, levels = percentile_columns),
      latency_ns = latency_seconds * 1e9
    ) %>%
    group_by(metric, manager, fault_label, percentile) %>%
    summarise(
      latency_mean = mean(latency_ns),
      latency_sd = ifelse(n() > 1, sd(latency_ns), 0),
      runs = n(),
      .groups = "drop"
    )

  latency_facets <- if (brief) {
    facet_wrap(vars(percentile), scales = "free_y", ncol = 4)
  } else {
    facet_wrap(vars(metric, percentile), scales = "free_y", ncol = 4)
  }

  dodge <- position_dodge(width = 0.75)
  latency_plot <- ggplot(
    latency,
    aes(x = fault_label, y = latency_mean, fill = manager)
  ) +
    geom_col(position = dodge, width = 0.65) +
    geom_errorbar(
      aes(
        ymin = pmax(0, latency_mean - latency_sd),
        ymax = latency_mean + latency_sd
      ),
      position = dodge,
      width = 0.2
    ) +
    latency_facets +
    labs(
      title = if (brief) NULL else sprintf(
        "Manager latency: %s CPU, %s clients, %s QPS, %s address(es), reorg=%s",
        values$gomaxprocs, values$clients, values$qps, values$addrs, values$reorg
      ),
      x = "Transaction failure probability",
      y = "Latency (ns)",
      fill = "Manager"
    ) +
    theme_bw(base_size = 12) +
    theme(legend.position = "bottom")

  latency_prefix <- if (brief) "latency_brief_" else "latency_"
  latency_height <- if (brief) 4.5 else 8
  latency_path <- file.path("assets", paste0(latency_prefix, suffix, ".pdf"))
  ggsave(latency_path, latency_plot, width = 13, height = latency_height, device = "pdf")
  message(latency_path)

  if (brief) {
    figure_label <- paste0(
      "fig:latency-brief-",
      gsub("[^[:alnum:]-]+", "-", suffix)
    )
    figure_caption <- sprintf(
      "Latency with %s CPU cores, %s clients, %s target QPS, %s address(es), and reorganization set to %s.",
      values$gomaxprocs,
      values$clients,
      values$qps,
      values$addrs,
      tolower(as.character(values$reorg))
    )
    brief_figure_blocks <- c(
      brief_figure_blocks,
      paste(
        "\\begin{figure}[p]",
        "  \\centering",
        sprintf("  \\includegraphics[width=\\linewidth]{%s}", latency_path),
        sprintf("  \\caption{%s}", figure_caption),
        sprintf("  \\label{%s}", figure_label),
        "\\end{figure}",
        sep = "\n"
      )
    )
    next
  }

  throughput <- scenario %>%
    group_by(manager, fault_label) %>%
    summarise(
      qps_mean = mean(achieved_qps),
      qps_sd = ifelse(n() > 1, sd(achieved_qps), 0),
      .groups = "drop"
    )

  throughput_plot <- ggplot(
    throughput,
    aes(x = fault_label, y = qps_mean, color = manager, group = manager)
  ) +
    geom_line(linewidth = 0.8) +
    geom_point(size = 2) +
    geom_errorbar(
      aes(ymin = pmax(0, qps_mean - qps_sd), ymax = qps_mean + qps_sd),
      width = 0.08
    ) +
    geom_hline(yintercept = values$qps, linetype = "dashed", color = "grey50") +
    labs(
      title = sprintf("Achieved throughput: %s", suffix),
      x = "Transaction failure probability",
      y = "Completed operations per second",
      color = "Manager"
    ) +
    theme_bw(base_size = 12) +
    theme(legend.position = "bottom")

  throughput_path <- file.path("assets", paste0("throughput_", suffix, ".pdf"))
  ggsave(throughput_path, throughput_plot, width = 8, height = 4.5, device = "pdf")
  message(throughput_path)
}

if (brief) {
  latex_path <- file.path("assets", "latency_brief_figures.tex")
  writeLines(
    c(
      "% Generated by Rscript plots.R --brief; requires \\usepackage{graphicx}.",
      "",
      paste(brief_figure_blocks, collapse = "\n\n")
    ),
    latex_path
  )
  message(latex_path)
}
