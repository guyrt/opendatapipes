# local runner to cheaply iterate over days 1 at at time

$startString = $args[0]
$numDays = $args[1]

az ml compute start --name smallStandard --resource-group rg-fecdata --workspace fecaml

$num_failures = 0

$startDate = [datetime]::parseexact($startString, 'yyyyMMdd', $null)
$endDate = $startDate.AddDays($numDays)

$days = New-Object Collections.Generic.List[string]

while ($startDate -lt $endDate) {
    $days.Add($startDate.ToString('yyyyMMdd'))
    $startDate = $startDate.AddDays(1)
}

foreach  ($run_date in $days) {
    echo "Start on $run_date";
    # hangs up if you try to save output.
    az ml job create --file .\fecUnzipToMergePipeline.yaml --resource-group rg-fecdata --workspace fecaml --set inputs.run_date=$run_date > /tmp/az.json

    $run_obj = cat /tmp/az.json | ConvertFrom-Json
    $run_name = $run_obj.name

    echo "Running $run_date named $run_name";

    function get_status($name) {
        $az_status_obj = az ml job show --name $run_name --resource-group rg-fecdata --workspace fecaml | ConvertFrom-Json

        $az_status = $az_status_obj.status
        return $az_status
    }

    $az_status = get_status($run_name)

    $last_date_check = Get-Date
    while ($az_status -eq "Running") {
        $now = Get-Date
        if (($now - $last_date_check).TotalMinutes -gt 5) {
            echo "$az_status at $now"
            $last_date_check = Get-Date
        }
        Start-Sleep -s 60
        $az_status_obj = az ml job show --name $run_name --resource-group rg-fecdata --workspace fecaml | ConvertFrom-Json
        $az_status = get_status($run_name)
    }

    echo "$run_date had $az_status"
    if ($az_status -eq "Failed") {
        $num_failures = $num_failures + 1
        if ($num_failures -gt 3) {
            break;
        }
    }
}