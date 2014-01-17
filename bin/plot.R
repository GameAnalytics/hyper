#!/usr/bin/Rscript --vanilla

require("ggplot2")
require("scales")

stats = read.csv("estimates.csv")


p_labeller <- function (var, value) {
  return (paste("p=", value))
}

p <- ggplot(data = stats, aes(x = card, y = median)) +
  geom_point(aes(colour = factor(p))) +
  geom_errorbar(aes(ymax = p95, ymin = p05, colour = factor(p)), alpha = 0.8, size=0.2) +
  scale_x_log10(breaks = unique(stats$card), labels = comma) +
  ##scale_x_continuous(breaks = seq(0, max(stats$card), by = 5000), labels = comma) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ##scale_y_continuous(breaks = c(0:100 / 1000), labels = percent) +
  scale_y_continuous(labels = percent) +
  facet_grid(. ~ p)


ggsave(file = "hyper.png", width = 10, height = 6)
