$(function() {
    $('div#addTableButton').bind('click', function() {
        $.getJSON($SCRIPT_ROOT + '/_get_aggregate_nearest_neighbors_membership', {
            query_key: QUERY_KEY,
            current_corpora: CURRENT_CORPORA
        }, function(data) {
            $('#sourceSelectionTable').innerHTML = "";
            for (var i=0; i < data.length; i++) {
                if (data[i]["Checked"] == 1)
                    checkbox_state="checked='checked'";
                else
                    checkbox_state="";
                var markup="<tr><td><input type='checkbox' name='corpora' "
                    + checkbox_state
                    + " value='"
                    + data[i]["Source"]
                    + "' /></td><td>"
                    + data[i]["Source"]
                    + "</td></tr>";
                $('#sourceSelectionTable').append(markup);
            }
            $('#sourceSelectionPanel').show();
        });
        return false;
    });
});
