USE `world`;

CREATE TABLE `continent`(
    `Name` enum('Asia','Europe','North America','Africa','Oceania','Antarctica','South America') NOT NULL DEFAULT 'Asia',
    `Area` INT NOT NULL DEFAULT '0',
    `PercentTotalMass` DECIMAL(10,2) NOT NULL DEFAULT '0.00',
    `MostPopulousCity` INT NOT NULL DEFAULT '0',
    PRIMARY KEY (`Name`),
    FOREIGN KEY (`MostPopulousCity`) REFERENCES `city` (`ID`)
)
