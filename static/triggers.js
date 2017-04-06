$("#select_cdb").change(function() {
   
    var cdb_id = $(this).find(":selected").val();
    
    var request = $.ajax({
         type: 'GET',
         url: '/lists/' + cdb_id + '/',
    });


    request.done(function(lists){
        
        var option_list = [["None", "Select a list:"]].concat(lists);

        $("#select_lst").empty();
        for (var i = 0; i < option_list.length; i++) {
            $("#select_lst").append(
                $("<option></option>").attr(
                    "value", option_list[i][0]).text(option_list[i][1])
            );
        }
    });

   
});







