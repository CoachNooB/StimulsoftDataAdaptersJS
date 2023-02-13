<?php
# Stimulsoft.Reports.JS
# Version: 2023.1.7
# Build date: 2023.02.10
# License: https://www.stimulsoft.com/en/licensing/reports
?>
<?php

namespace Stimulsoft;

class StiResponse
{
    public static function json($result, $encode = false)
    {
        unset($result->object);
        $result = defined('JSON_UNESCAPED_SLASHES') ? json_encode($result, JSON_UNESCAPED_SLASHES) : json_encode($result);
        echo $encode ? str_rot13(base64_encode($result)) : $result;
    }
}