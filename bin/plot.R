#!/usr/bin/Rscript --vanilla

require("ggplot2")
require("scales")

stats = read.csv("data.csv")


p_labeller <- function (var, value) {
  memory <- (2^value) / 1024
  return (paste("p=", value, " (", memory, "k)", sep = ""))
}


p <- ggplot(data = stats, aes(x = card, y = mean / card)) +
  geom_text(aes(label = paste(round((mean / card) * 100, 2), "%")), size = 2, angle = 45) +
  scale_x_log10(breaks = c(100, 1000, 10000, 100000, 1000000), labels = comma) +
  scale_y_continuous(breaks = c(0:100 / 100), labels = percent) +
  facet_grid(. ~ p, labeller = p_labeller) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))

ggsave(file = "hyper.pdf", width = 10, height = 6)